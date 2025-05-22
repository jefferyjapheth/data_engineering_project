import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
from pyspark.sql.types import StringType, IntegerType, DoubleType


def load_env_variables():
    return {
        "POSTGRES_URL": os.getenv("POSTGRES_URL"),  # e.g. jdbc:postgresql://postgres_labb:5432/flight_price_db
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_DRIVER": os.getenv("POSTGRES_DRIVER", "org.postgresql.Driver"),
        "POSTGRES_TABLE": os.getenv("TABLE_NAME"),  # final table base name

        "MYSQL_URL": f"jdbc:mysql://mysql:3306/{os.getenv('MYSQL_DATABASE')}",
        "MYSQL_USER": os.getenv("MYSQL_USER"),
        "MYSQL_PASSWORD": os.getenv("MYSQL_PASSWORD"),
        "MYSQL_TABLE": os.getenv("MYSQL_TABLE_NAME"),

        "PARTITION_COLUMN": "Departure Date & Time"
    }


def init_spark():
    return (SparkSession.builder
            .appName("Flight Price Data Transformation")
            .config("spark.sql.shuffle.partitions", "8")  # reasonable for mid-sized data
            .config("spark.sql.adaptive.enabled", "true")  # optimize query plans dynamically
            .getOrCreate())


def get_partition_bounds(spark, env_vars):
    df_sample = (spark.read.format("jdbc")
                 .option("url", env_vars["MYSQL_URL"])
                 .option("dbtable", env_vars["MYSQL_TABLE"])
                 .option("user", env_vars["MYSQL_USER"])
                 .option("password", env_vars["MYSQL_PASSWORD"])
                 .option("driver", "com.mysql.cj.jdbc.Driver")
                 .load())

    bounds = df_sample.selectExpr(
        f"min(`{env_vars['PARTITION_COLUMN']}`) as lowerBound",
        f"max(`{env_vars['PARTITION_COLUMN']}`) as upperBound"
    ).collect()[0]
    return bounds['lowerBound'], bounds['upperBound']


def read_mysql_data(spark, env_vars, lower_bound, upper_bound):
    return (spark.read.format("jdbc")
            .option("url", env_vars["MYSQL_URL"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", env_vars["MYSQL_TABLE"])
            .option("user", env_vars["MYSQL_USER"])
            .option("password", env_vars["MYSQL_PASSWORD"])
            .option("fetchSize", 10000)
            .option("partitionColumn", f"`{env_vars['PARTITION_COLUMN']}`")
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
            .option("numPartitions", 4)
            .load())


def enforce_schema(df):
    schema_map = {
        "Airline": StringType(),
        "Source": StringType(),
        "Source Name": StringType(),
        "Destination": StringType(),
        "Destination Name": StringType(),
        "Departure Date & Time": ("timestamp", "yyyy-MM-dd HH:mm:ss"),
        "Arrival Date & Time": ("timestamp", "yyyy-MM-dd HH:mm:ss"),
        "Duration (hrs)": DoubleType(),
        "Stopovers": StringType(),
        "Aircraft Type": StringType(),
        "Class": StringType(),
        "Booking Source": StringType(),
        "Base Fare (BDT)": DoubleType(),
        "Tax & Surcharge (BDT)": DoubleType(),
        "Total Fare (BDT)": DoubleType(),
        "Seasonality": StringType(),
        "Days Before Departure": IntegerType(),
    }

    selected_cols = []
    for c, t in schema_map.items():
        if isinstance(t, tuple) and t[0] == "timestamp":
            fmt = t[1]
            selected_cols.append(to_timestamp(col(c), fmt).alias(c))
        else:
            selected_cols.append(col(c).cast(t).alias(c))

    return df.select(selected_cols)


def clean_data(df):
    df = df.dropna(subset=["Airline", "Source", "Destination", "Base Fare (BDT)", "Tax & Surcharge (BDT)"])

    df = df.withColumn(
        "Total Fare (BDT)",
        when(col("Total Fare (BDT)").isNull(),
             col("Base Fare (BDT)") + col("Tax & Surcharge (BDT)"))
        .otherwise(col("Total Fare (BDT)"))
    )

    df = df.filter(
        (col("Base Fare (BDT)") >= 0) &
        (col("Tax & Surcharge (BDT)") >= 0) &
        (col("Total Fare (BDT)") >= 0)
    )

    return df


def compute_kpis(spark):
    return {
        "avg_fare_by_airline": spark.sql("""
            SELECT Airline, AVG(`Total Fare (BDT)`) AS avg_total_fare
            FROM flight_prices_cleaned
            GROUP BY Airline
        """),
        "seasonal_variation": spark.sql("""
            SELECT Seasonality, AVG(`Total Fare (BDT)`) AS avg_fare
            FROM flight_prices_cleaned
            GROUP BY Seasonality
        """),
        "booking_count": spark.sql("""
            SELECT Airline, COUNT(*) AS booking_count
            FROM flight_prices_cleaned
            GROUP BY Airline
        """),
        "popular_routes": spark.sql("""
            SELECT Source, Destination, COUNT(*) AS route_count
            FROM flight_prices_cleaned
            GROUP BY Source, Destination
            ORDER BY route_count DESC
        """)
    }


def write_to_postgres(df_dict, df_main, env_vars):
    pg_properties = {
        "user": env_vars["POSTGRES_USER"],
        "password": env_vars["POSTGRES_PASSWORD"],
        "driver": env_vars["POSTGRES_DRIVER"]
    }

    # Write main cleaned data
    df_main.write.jdbc(env_vars["POSTGRES_URL"], env_vars["POSTGRES_TABLE"] + "_cleaned", mode="overwrite", properties=pg_properties)

    # Write KPI tables
    for name, df in df_dict.items():
        df.write.jdbc(env_vars["POSTGRES_URL"], f"{env_vars['POSTGRES_TABLE']}_kpi_{name}", mode="overwrite", properties=pg_properties)


def main():
    env_vars = load_env_variables()
    spark = init_spark()

    lower_bound, upper_bound = get_partition_bounds(spark, env_vars)
    df_raw = read_mysql_data(spark, env_vars, lower_bound, upper_bound)

    df_casted = enforce_schema(df_raw)
    df_cleaned = clean_data(df_casted)

    df_cleaned.cache()
    df_cleaned.createOrReplaceTempView("flight_prices_cleaned")
    df_cleaned.count()  # materialize cache

    kpis = compute_kpis(spark)

    write_to_postgres(kpis, df_cleaned, env_vars)

    spark.stop()


if __name__ == "__main__":
    main()

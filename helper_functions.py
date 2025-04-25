from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when,col, split, explode, year, mean, collect_list, array, lit, avg, round, concat_ws, when, lower, trim, regexp_replace, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType,LongType, IntegerType, FloatType, ArrayType, MapType, BooleanType
import os
import requests
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


# Function to initialize Spark session
def initialize_spark():
    spark = SparkSession.builder \
        .appName("TMDB Movie Data Processing") \
        .getOrCreate()
    return spark


# Function to define schema for DataFrame to tell pyspark the datatypes it's fetching from the api for accuracy and efficiency
def get_schema():
    return StructType([
        # Movie base fields
        StructField("adult", BooleanType(), True),
        StructField("backdrop_path", StringType(), True),
        StructField("belongs_to_collection", MapType(StringType(), StringType()), True),
        StructField("budget", LongType(), True),
        StructField("genres", ArrayType(MapType(StringType(), StringType())), True),
        StructField("homepage", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("popularity", FloatType(), True),
        StructField("poster_path", StringType(), True),
        StructField("production_companies", ArrayType(MapType(StringType(), StringType())), True),
        StructField("production_countries", ArrayType(MapType(StringType(), StringType())), True),
        StructField("release_date", StringType(), True),
        StructField("revenue", LongType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("spoken_languages", ArrayType(MapType(StringType(), StringType())), True),
        StructField("status", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("title", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("vote_average", FloatType(), True),
        StructField("vote_count", IntegerType(), True),

        # Nested data for credits
        StructField("credits", StructType([
            StructField("id", IntegerType(), True),
            StructField("cast", ArrayType(StructType([
                StructField("adult", BooleanType(), True),
                StructField("gender", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("original_name", StringType(), True),
                StructField("popularity", FloatType(), True),
                StructField("profile_path", StringType(), True),
                StructField("cast_id", IntegerType(), True),
                StructField("character", StringType(), True),
                StructField("credit_id", StringType(), True),
                StructField("order", IntegerType(), True)
            ]), True)),
            StructField("crew", ArrayType(StructType([
                StructField("adult", BooleanType(), True),
                StructField("gender", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("original_name", StringType(), True),
                StructField("popularity", FloatType(), True),
                StructField("profile_path", StringType(), True),
                StructField("credit_id", StringType(), True),
                StructField("department", StringType(), True),
                StructField("job", StringType(), True)
            ]), True))
        ]), True),

        # Nested data for reviews
        StructField("reviews", StructType([
            StructField("id", IntegerType(), True),
            StructField("page", IntegerType(), True),
            StructField("results", ArrayType(StructType([
                StructField("author", StringType(), True),
                StructField("author_details", StructType([
                    StructField("name", StringType(), True),
                    StructField("username", StringType(), True),
                    StructField("avatar_path", StringType(), True),
                    StructField("rating", FloatType(), True)
                ]), True),
                StructField("content", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("id", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("url", StringType(), True)
            ]), True)),
            StructField("total_pages", IntegerType(), True),
            StructField("total_results", IntegerType(), True)
        ]), True)
    ])

# Function to fetch movie data from the TMDB API
def fetch_single_movie(movie_id, headers):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=reviews,credits"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            logging.info(f"Successfully fetched movie ID: {movie_id}")
            return response.json()
        else:
            logging.warning(f"Failed to fetch movie ID: {movie_id}, Status code: {response.status_code}") #helps with monitoring logs
            return None
    except Exception as e:
        logging.error(f"Error fetching movie ID {movie_id}: {e}")
        return None

#This function makes use of threadpool to parallelize the data extraction from the api. it iterates through the movie id list and gets them from the url
def fetch_movie_data(movie_ids, api_token, max_workers=10):
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    movies_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {executor.submit(fetch_single_movie, movie_id, headers): movie_id for movie_id in movie_ids}
        
        for future in as_completed(future_to_id):
            movie_data = future.result()
            if movie_data:
                movies_data.append(movie_data)

    return movies_data


# Function for loading the saved parquet data 
def load_from_parquet(file_path, spark):
    if os.path.exists(file_path):
        df = spark.read.parquet(file_path)
        print(f"Loaded data from {file_path}")
        return df
    else:
        print(f"File not found: {file_path}")
        return None


# Function to save DataFrame to Parquet 
def save_to_parquet(df, file_path):
    df.write.mode("overwrite").parquet(file_path)
    print(f"Data saved to {file_path}")



#DEFINING FUNCTIONS TO CLEAN AND TRANSFORM DATA

# Main process to fetch and save movie data
def process_movie_data(spark, movie_ids=None):
    
    logging.basicConfig(level=logging.INFO)

    parquet_file_path = "./dataset/raw/raw_tmdb_movies.parquet"
    schema = get_schema()

    # Check if the Parquet file exists
    raw_movies_df = load_from_parquet(parquet_file_path, spark)

    if raw_movies_df is not None:
        logging.info("Parquet file loaded successfully.")
        return raw_movies_df

    api_token = os.getenv("TMDB_API_KEY")
    if not api_token:
        raise EnvironmentError("TMDB_API_KEY not found in environment variables.")

    if movie_ids is None:
        movie_ids = [0, 299534, 19995, 140607, 299536, 597, 135397,
                     420818, 24428, 168259, 99861, 284054, 12445,
                     181808, 330457, 351286, 109445, 321612, 260513]

    movies_data = fetch_movie_data(movie_ids, api_token)

    if not movies_data:
        raise ValueError("No movie data retrieved. Cannot create DataFrame.")

    try:
        raw_movies_df = spark.createDataFrame(movies_data, schema)
        save_to_parquet(raw_movies_df, parquet_file_path)
        logging.info("DataFrame created and saved to Parquet.")
    except Exception as e:
        logging.error(f"Failed to create/save DataFrame: {e}")
        raise

    return raw_movies_df




# Function to extract movie columns from nested data
def get_movie_columns(df):
    """Extract additional columns from nested credits and reviews data"""
    
    logging.info("Starting to extract movie columns.")

    try:
        # Extract cast_size
        df = df.withColumn("cast_size", F.size(F.col("credits.cast")))
        
        # Extract crew_size
        df = df.withColumn("crew_size", F.size(F.col("credits.crew")))

        # Extract directors
        df = df.withColumn("directors", F.expr("transform(filter(credits.crew, c -> c.job = 'Director'), d -> d.name)"))

        # Extract cast names
        df = df.withColumn("cast", F.expr("transform(credits.cast, c -> c.name)"))

        # Calculate average rating from reviews
        df = df.withColumn("rating", 
                           F.when(F.size(F.expr("filter(reviews.results, r -> r.author_details.rating is not null)")) > 0,
                                  F.round(F.expr(""" 
                                      aggregate(
                                          filter(reviews.results, r -> r.author_details.rating is not null), 
                                          cast(0 as double), 
                                          (acc, r) -> acc + cast(r.author_details.rating as double), 
                                          sum -> sum / size(filter(reviews.results, r -> r.author_details.rating is not null))
                                      )
                                  """), 2)
                           ).otherwise(None))

        logging.info("Movie columns extraction completed successfully.")
        return df

    except Exception as e:
        logging.error(f"Error during movie columns extraction: {e}")
        raise

# Function to clean movie data
def clean_movie_data(df, pipe_columns=None, collection_col=None, columns_to_drop=None):
    """Clean movie data with essential transformations"""
    
    logging.info("Starting data cleaning process.")

    try:
        # Process pipe-separated columns
        if pipe_columns:
            for col_name in pipe_columns:
                if col_name in df.columns:
                    df = df.withColumn(col_name, 
                                      F.concat_ws("|", F.expr(f"transform({col_name}, x -> x.name)")))

        # Process collection name
        if collection_col and collection_col in df.columns:
            df = df.withColumn(collection_col, 
                               F.col(f"{collection_col}.name"))
        
        # Drop irrelevant columns
        if columns_to_drop:
            df = df.drop(*columns_to_drop)

        logging.info("Data cleaning completed successfully.")
        return df

    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        raise


# FUNCTION TO HANDLE INCORRECT AND MISSING DATA
def handle_incorrect_data(extracted_df, numeric_columns=None, money_columns=None, placeholders=None):
    """Handle incorrect or missing data in movie dataset"""
    
    logging.info("Starting data correction process.")

    try:
        # Replace zeros with null for numeric columns
        for col_name in numeric_columns:
            extracted_df = extracted_df.withColumn(
                col_name,
                F.when((F.col(col_name).isNotNull()) & (F.col(col_name) != 0), F.col(col_name)).otherwise(None)
            )

        # Convert money columns to millions
        for col_name in money_columns:
            if col_name in extracted_df.columns:
                extracted_df = extracted_df.withColumn(col_name, F.col(col_name) / 1000000)

        # Rename money columns to include _musd suffix
        for old_name in money_columns:
            new_name = f"{old_name}_musd"
            extracted_df = extracted_df.withColumnRenamed(old_name, new_name)

        # Process string columns to replace placeholders with NaN
        string_cols = [f.name for f in extracted_df.schema.fields if isinstance(f.dataType, StringType)]

        for col_name in string_cols:
            # Replace placeholder values with NaN
            extracted_df = extracted_df.withColumn(
                col_name,
                F.when(~F.col(col_name).isin(placeholders) & F.col(col_name).isNotNull(),
                      F.col(col_name)).otherwise(None)
            )
        
        # Save the final processed data
        extracted_df.write.mode("overwrite").parquet("./dataset/processed/tmdb_movies_processed.parquet")
        logging.info("Processed movie data saved to tmdb_movies_processed.parquet")

        return extracted_df

    except Exception as e:
        logging.error(f"Error during data correction: {e}")
        raise


#FUNCTION TO REORDER COLUMNS
def reorder_columns(df, desired_order):
    """Reorders columns in a DataFrame according to a desired order and caches the result."""
    
    logging.info("Starting to reorder columns.")

    try:
        existing_columns = set(df.columns)
        final_columns = [col for col in desired_order if col in existing_columns] + \
                        [col for col in df.columns if col not in set(desired_order)]

        df = df.select(*final_columns)
        df.cache()

        logging.info("Column reordering completed successfully.")
        return df

    except Exception as e:
        logging.error(f"Error during column reordering: {e}")
        raise



# FUNCTION TO GET MOVIE RANKINGS
def get_movie_rankings(df, spark):
    """Generate rankings for movies based on various metrics"""
    
    logging.info("Starting movie rankings generation.")

    try:
        df.createOrReplaceTempView("movies")
        queries = {
            "Highest Revenue": "SELECT * FROM movies ORDER BY revenue_musd DESC",
            "Highest Budget": "SELECT * FROM movies ORDER BY budget_musd DESC",
            "Highest Profit": "SELECT * FROM movies ORDER BY profit DESC",
            "Lowest Profit": "SELECT * FROM movies ORDER BY profit ASC",
            "Highest ROI (Budget ≥ 10M)": "SELECT * FROM movies WHERE budget_musd >= 10 ORDER BY roi DESC",
            "Lowest ROI (Budget ≥ 10M)": "SELECT * FROM movies WHERE budget_musd >= 10 ORDER BY roi ASC",
            "Most Voted Movies": "SELECT * FROM movies ORDER BY vote_count DESC",
            "Highest Rated Movies (≥10 votes)": "SELECT * FROM movies WHERE vote_count >= 10 ORDER BY rating DESC",
            "Lowest Rated Movies (≥10 votes)": "SELECT * FROM movies WHERE vote_count >= 10 ORDER BY rating ASC",
            "Most Popular Movies": "SELECT * FROM movies ORDER BY popularity DESC",
            "Shortest Runtime Movies": "SELECT * FROM movies ORDER BY runtime ASC"
        }

        rankings = {}
        for name, query in queries.items():
            rankings[name] = spark.sql(query)

        logging.info("Movie rankings generated successfully.")
        return rankings

    except Exception as e:
        logging.error(f"Error during rankings generation: {e}")
        raise

# THIS FUNCTION RETURNS THE PERFORMANCE OF FRANCHISE MOVIES BY THE METRICS/STATS SPECIFIED AS PARAMETERS
"""
    if no parameter is specified it filters based on budget,revenue and rating
"""
def franchise_stats(spark, *, budget=None, revenue=None, rating=None, limit=10):
    logging.info("Running franchise stats...")

    if all(v is None for v in [budget, revenue, rating]):
        budget = revenue = rating = True

    fields = ["belongs_to_collection", "COUNT(*) AS movie_count"]

    if budget:
        fields += [
            "SUM(budget_musd) AS total_budget",
            "AVG(budget_musd) AS avg_budget"
        ]
    if revenue:
        fields += [
            "SUM(revenue_musd) AS total_revenue",
            "AVG(revenue_musd) AS avg_revenue"
        ]
    if rating:
        fields.append("AVG(vote_average) AS avg_rating")

    select_clause = ",\n    ".join(fields)
    order_by = "total_revenue" if revenue else "movie_count"

    query = f"""
        SELECT 
            {select_clause}
        FROM movies
        WHERE belongs_to_collection IS NOT NULL
        GROUP BY belongs_to_collection
        ORDER BY {order_by} DESC
        LIMIT {limit}
    """
    return spark.sql(query)

# THIS FUNCTION RETURNS THE PERFORMANCE OF DIRECTORS BY THE METRICS/STATS SPECIFIED AS PARAMETERS
"""
    if no parameter is specified it filters based on budget,revenue and rating
"""
def director_stats(spark, *, revenue=None, rating=None, limit=10):
    logging.info("Running director stats...")

    if all(v is None for v in [revenue, rating]):
        revenue = rating = True

    fields = ["directors", "COUNT(*) AS movie_count"]

    if revenue:
        fields.append("SUM(revenue_musd) AS total_revenue")
    if rating:
        fields.append("AVG(vote_average) AS avg_rating")

    select_clause = ",\n    ".join(fields)
    order_by = "total_revenue" if revenue else "movie_count"

    query = f"""
        SELECT 
            {select_clause}
        FROM movies
        WHERE directors IS NOT NULL
        GROUP BY directors
        ORDER BY {order_by} DESC
        LIMIT {limit}
    """
    return spark.sql(query)

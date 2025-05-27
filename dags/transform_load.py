from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import io  # Added for pd.read_json fix

logger = LoggingMixin().log

def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Validating and cleaning data...")
    required_cols = [
        "Airline", "Source", "Destination",
        "Base Fare (BDT)", "Tax & Surcharge (BDT)", "Total Fare (BDT)"
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df.dropna(subset=required_cols[:5])
    fare_cols = ["Base Fare (BDT)", "Tax & Surcharge (BDT)", "Total Fare (BDT)"]
    df[fare_cols] = df[fare_cols].apply(pd.to_numeric, errors="coerce")

    df["Total Fare (BDT)"] = df.apply(
        lambda r: r["Base Fare (BDT)"] + r["Tax & Surcharge (BDT)"]
        if pd.isna(r["Total Fare (BDT)"]) else r["Total Fare (BDT)"],
        axis=1,
    )

    df = df.query(
        "`Base Fare (BDT)` >= 0 and `Tax & Surcharge (BDT)` >= 0 and `Total Fare (BDT)` >= 0"
    )

    for col in ["Airline", "Source", "Destination"]:
        df[col] = df[col].astype(str).str.strip()

    logger.info("Validation and cleaning complete.")
    return df

def enrich_with_seasonality(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding seasonality column...")
    df["Departure Date & Time"] = pd.to_datetime(df["Departure Date & Time"], errors="coerce")

    def classify_season(dt):
        if pd.isna(dt):
            return "Unknown"
        if dt.month in [12, 1]:
            return "Winter"
        if dt.month in [4, 5]:
            return "Eid"
        return "Off-Peak"

    df["Seasonality"] = df["Departure Date & Time"].apply(classify_season)
    logger.info("Seasonality enrichment done.")
    return df

def compute_kpis(df: pd.DataFrame):
    logger.info("Computing KPIs...")
    kpis = {
        "avg_fare_by_airline": df.groupby("Airline")["Total Fare (BDT)"].mean().reset_index(),
        "seasonal_variation": df.groupby("Seasonality")["Total Fare (BDT)"].mean().reset_index(),
        "booking_count": df.groupby("Airline").size().reset_index(name="booking_count"),
        "popular_routes": df.groupby(["Source", "Destination"]).size()
                            .reset_index(name="route_count")
                            .sort_values("route_count", ascending=False),
    }
    logger.info("KPIs computed.")
    return kpis

def write_to_postgres(df_main, kpis, postgres_url, postgres_table):
    logger.info("Writing data to Postgres...")
    engine = create_engine(postgres_url)
    df_main.to_sql(f"{postgres_table}_cleaned", con=engine, if_exists="replace", index=False)
    logger.info(f"Written cleaned data to {postgres_table}_cleaned")
    for kpi_name, df_kpi in kpis.items():
        target_table = f"{postgres_table}_kpi_{kpi_name}"
        df_kpi.to_sql(target_table, con=engine, if_exists="replace", index=False)
        logger.info(f"Written KPI '{kpi_name}' to {target_table}")
    logger.info("All data written to Postgres successfully.")

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["flight", "kpi", "postgresql", "transform"],
)
def transform_load_dag():
    @task()
    def read_from_mysql(mysql_url: str, mysql_table: str) -> str:
        engine = create_engine(mysql_url)
        df = pd.read_sql_table(mysql_table, con=engine)
        logger.info(f"Read {len(df)} rows from MySQL table {mysql_table}")
        return df.to_json(orient="records")

    @task()
    def clean_and_enrich(df_json: str) -> str:
        df = pd.read_json(io.StringIO(df_json), orient="records")
        df_clean = validate_and_clean_data(df)
        df_enriched = enrich_with_seasonality(df_clean)
        return df_enriched.to_json(orient="records")

    @task()
    def compute_and_write_kpis(df_json: str) -> str:
        df = pd.read_json(io.StringIO(df_json), orient="records")
        kpis = compute_kpis(df)
        postgres_url = Variable.get("postgres_conn_uri", default_var="postgresql+psycopg2://postgres:postgres@postgres/postgres")
        postgres_table = Variable.get("postgres_table", default_var="flight_prices")
        write_to_postgres(df, kpis, postgres_url, postgres_table)
        return "Success"

    MYSQL_CONN_URI = Variable.get(
        "mysql_conn_uri", default_var="mysql+mysqlconnector://mysql:amalitech@mysql/staging_db"
    )

    df_json = read_from_mysql(MYSQL_CONN_URI, "flight_prices")
    cleaned_json = clean_and_enrich(df_json)
    compute_and_write_kpis(cleaned_json)

transform_load_dag()

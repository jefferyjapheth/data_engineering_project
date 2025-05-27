import os
from functools import lru_cache
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine
from scripts.logger import get_logger

logger = get_logger("flight_price_etl")

def load_env_variables() -> Dict[str, str]:
    """Load required environment variables."""
    logger.info("Loading environment variables...")
    env_vars = {
        "POSTGRES_URL": os.getenv("POSTGRES_URL"),
        "POSTGRES_TABLE": os.getenv("TABLE_NAME"),
        "MYSQL_URL": os.getenv("MYSQL_URL"),
        "MYSQL_TABLE": os.getenv("MYSQL_TABLE_NAME"),
    }

    missing = [k for k, v in env_vars.items() if not v]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        raise EnvironmentError(f"Missing environment variables: {missing}")
    
    logger.info("Environment variables loaded successfully.")
    return env_vars


@lru_cache(maxsize=1)
def get_engine(db_url: str):
    """Create and cache SQLAlchemy engine."""
    try:
        logger.info(f"Creating database engine for URL: {db_url}")
        return create_engine(db_url)
    except Exception as e:
        logger.exception(f"Error creating engine for {db_url}")
        raise


@lru_cache(maxsize=1)
def read_mysql_data(mysql_url: str, mysql_table: str) -> pd.DataFrame:
    """Read table from MySQL with caching."""
    try:
        logger.info(f"Reading data from MySQL table: {mysql_table}")
        engine = get_engine(mysql_url)
        df = pd.read_sql_table(mysql_table, con=engine)
        logger.info(f"Successfully read {len(df)} rows from {mysql_table}")
        return df
    except Exception as e:
        logger.exception(f"Failed to read table {mysql_table} from MySQL")
        raise


def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean flight fare data."""
    logger.info("Validating and cleaning data...")
    required_cols = [
        "Airline", "Source", "Destination",
        "Base Fare (BDT)", "Tax & Surcharge (BDT)", "Total Fare (BDT)"
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    try:
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

        logger.info("Data validation and cleaning completed.")
        return df
    except Exception as e:
        logger.exception("Data validation and cleaning failed.")
        raise


def enrich_with_seasonality(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'Seasonality' column based on 'Departure Date & Time'."""
    logger.info("Enriching data with seasonality information...")
    try:
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
        logger.info("Seasonality enrichment complete.")
        return df
    except Exception as e:
        logger.exception("Failed to enrich data with seasonality.")
        raise


def compute_kpis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Compute KPIs from the flight data."""
    logger.info("Computing KPIs from data...")
    try:
        kpis = {
            "avg_fare_by_airline": df.groupby("Airline")["Total Fare (BDT)"].mean().reset_index(),
            "seasonal_variation": df.groupby("Seasonality")["Total Fare (BDT)"].mean().reset_index(),
            "booking_count": df.groupby("Airline").size().reset_index(name="booking_count"),
            "popular_routes": df.groupby(["Source", "Destination"]).size()
                                .reset_index(name="route_count")
                                .sort_values("route_count", ascending=False),
        }
        logger.info("KPI computation complete.")
        return kpis
    except Exception as e:
        logger.exception("Failed to compute KPIs.")
        raise


def write_to_postgres(
    df_main: pd.DataFrame,
    kpis: Dict[str, pd.DataFrame],
    postgres_url: str,
    postgres_table: str,
) -> None:
    """Write cleaned data and KPIs to PostgreSQL."""
    logger.info("Writing data to PostgreSQL...")
    try:
        engine = get_engine(postgres_url)
        df_main.to_sql(f"{postgres_table}_cleaned", con=engine, if_exists="replace", index=False)
        logger.info(f"Cleaned data written to table: {postgres_table}_cleaned")

        for kpi_name, df_kpi in kpis.items():
            target_table = f"{postgres_table}_kpi_{kpi_name}"
            df_kpi.to_sql(target_table, con=engine, if_exists="replace", index=False)
            logger.info(f"KPI '{kpi_name}' written to table: {target_table}")

        logger.info("All data successfully written to PostgreSQL.")
    except Exception as e:
        logger.exception("Failed to write data to PostgreSQL.")
        raise

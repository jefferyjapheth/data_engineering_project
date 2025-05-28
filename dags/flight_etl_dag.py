import os
from datetime import timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.utils.timezone import utcnow
from sqlalchemy import create_engine, inspect
from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.exceptions import AirflowFailException

from scripts.logger import get_logger

logger = get_logger("flight_etl_pipeline")
DATASET_FLIGHT_PRICES = Dataset("flight_prices")


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    start_date=utcnow() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["etl", "mysql"],
    params={"dataset": "mahatiratusher/flight-price-dataset-of-bangladesh"},
    default_args=DEFAULT_ARGS,
)
def download_and_stage_to_mysql():

    @task()
    def setup_data_dir():
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "mysql"))
            os.makedirs(data_dir, exist_ok=True)
            logger.info(f"Data directory created at: {data_dir}")
            return data_dir
        except Exception as e:
            logger.error(f"Failed to setup data directory: {e}")
            raise

    @task()
    def download_kaggle(dataset: str, data_dir: str):
        try:
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(dataset, path=data_dir, unzip=True)
            logger.info(f"Dataset {dataset} downloaded to {data_dir}")
            return True
        except Exception as e:
            logger.error(f"Kaggle download failed: {e}")
            raise

    @task()
    def find_csv_file(data_dir: str):
        try:
            for root, _, files in os.walk(data_dir):
                for file in files:
                    if file.endswith(".csv"):
                        path = os.path.join(root, file)
                        logger.info(f"CSV file found: {path}")
                        return path
            raise FileNotFoundError("CSV file not found in data directory")
        except Exception as e:
            logger.error(f"Finding CSV failed: {e}")
            raise

    @task()
    def read_csv(csv_path: str):
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"CSV file read with {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Reading CSV failed: {e}")
            raise

    @task()
    def create_mysql_url():
        try:
            user = os.getenv("MYSQL_USER", "mysql")
            password = os.getenv("MYSQL_PASSWORD", "amalitech")
            host = os.getenv("MYSQL_HOST", "mysql")
            db = os.getenv("MYSQL_DATABASE", "staging_db")
            url = f"mysql+mysqlconnector://{user}:{password}@{host}/{db}"
            logger.info("MySQL URL created")
            return url
        except Exception as e:
            logger.error(f"Creating MySQL URL failed: {e}")
            raise

    @task()
    def load_to_mysql(df: pd.DataFrame, mysql_url: str, table_name: str):
        try:
            engine = create_engine(mysql_url)
            df.to_sql(table_name, con=engine, if_exists="replace", index=False)
            logger.info(f"Data loaded to MySQL table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Loading to MySQL failed: {e}")
            raise

    @task()
    def validate(df: pd.DataFrame, mysql_url: str, table_name: str):
        try:
            engine = create_engine(mysql_url)
            inspector = inspect(engine)
            if table_name not in inspector.get_table_names():
                raise ValueError(f"Table '{table_name}' not found in MySQL")
            db_count = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", engine).iloc[0, 0]
            if db_count != len(df):
                raise ValueError(f"Row count mismatch: DataFrame={len(df)} vs DB={db_count}")
            logger.info("Data validation successful")
            return True
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    data_dir = setup_data_dir()
    dataset = "{{ params.dataset }}"
    download_kaggle(dataset, data_dir)
    csv_path = find_csv_file(data_dir)
    df = read_csv(csv_path)
    mysql_url = create_mysql_url()
    load_to_mysql(df, mysql_url, "flight_prices")
    validate_result = validate(df, mysql_url, "flight_prices")

    emit_dataset = EmptyOperator(task_id="emit_dataset", outlets=[DATASET_FLIGHT_PRICES])
    validate_result >> emit_dataset

@dag(
    schedule=[DATASET_FLIGHT_PRICES],
    start_date=utcnow() - timedelta(days=1),
    catchup=False,
    tags=["etl", "postgres"],
    default_args=DEFAULT_ARGS,
)
def transform_and_load_to_postgres():

    @task(multiple_outputs=True)
    def load_env():
        try:
            pg_url = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}@{os.getenv('POSTGRES_HOST', 'postgres')}/{os.getenv('POSTGRES_DB', 'postgres')}"
            mysql_url = f"mysql+mysqlconnector://{os.getenv('MYSQL_USER', 'mysql')}:{os.getenv('MYSQL_PASSWORD', 'amalitech')}@{os.getenv('MYSQL_HOST', 'mysql')}/{os.getenv('MYSQL_DATABASE', 'staging_db')}"
            logger.info("Environment variables loaded")
            return {
                "POSTGRES_URL": pg_url,
                "POSTGRES_TABLE": "flight_prices",
                "MYSQL_URL": mysql_url,
                "MYSQL_TABLE": "flight_prices",
            }
        except Exception as e:
            logger.error(f"Loading environment failed: {e}")
            raise

    @task()
    def read_mysql(mysql_url: str, mysql_table: str):
        try:
            engine = create_engine(mysql_url)
            df = pd.read_sql_table(mysql_table, con=engine)
            logger.info(f"Read {len(df)} rows from MySQL table: {mysql_table}")
            return df
        except Exception as e:
            logger.error(f"Reading from MySQL failed: {e}")
            raise

    @task()
    def clean_data(df: pd.DataFrame):
        try:
            fare_cols = ["Base Fare (BDT)", "Tax & Surcharge (BDT)", "Total Fare (BDT)"]
            df = df.dropna(subset=fare_cols[:2])
            df[fare_cols] = df[fare_cols].apply(pd.to_numeric, errors="coerce")
            df["Total Fare (BDT)"] = df.apply(
                lambda r: r[fare_cols[0]] + r[fare_cols[1]] if pd.isna(r[fare_cols[2]]) else r[fare_cols[2]],
                axis=1,
            )
            logger.info("Data cleaned")
            return df
        except Exception as e:
            logger.error(f"Cleaning data failed: {e}")
            raise

    @task()
    def enrich(df: pd.DataFrame):
        try:
            df["Departure Date & Time"] = pd.to_datetime(df["Departure Date & Time"], errors="coerce")
            def classify_season(dt):
                if pd.isna(dt): return "Unknown"
                if dt.month in [12, 1]: return "Winter"
                if dt.month in [4, 5]: return "Eid"
                return "Off-Peak"
            df["Seasonality"] = df["Departure Date & Time"].apply(classify_season)
            df["Route"] = df["Source Name"] + " - " + df["Destination Name"]
            logger.info("Data enriched")
            return df
        except Exception as e:
            logger.error(f"Enrichment failed: {e}")
            raise

    @task()
    def compute_kpis(df: pd.DataFrame):
        try:
            kpi_summary = (
                df.groupby(["Airline", "Route"])["Total Fare (BDT)"]
                .agg(["min", "max", "mean", "median"])
                .reset_index()
            )
            logger.info("KPIs computed")
            return kpi_summary
        except Exception as e:
            logger.error(f"KPI computation failed: {e}")
            raise

    @task()
    def load_to_postgres(df: pd.DataFrame, pg_url: str, pg_table: str):
        try:
            engine = create_engine(pg_url)
            df.to_sql(pg_table, con=engine, if_exists="replace", index=False)
            logger.info(f"Data loaded to PostgreSQL table: {pg_table}")
            return True
        except Exception as e:
            logger.error(f"Loading to PostgreSQL failed: {e}")
            raise

    env_vars = load_env()
    df_mysql = read_mysql(env_vars["MYSQL_URL"], env_vars["MYSQL_TABLE"])
    df_cleaned = clean_data(df_mysql)
    df_enriched = enrich(df_cleaned)
    df_kpis = compute_kpis(df_enriched)
    load_to_postgres(df_kpis, env_vars["POSTGRES_URL"], env_vars["POSTGRES_TABLE"])

# Instantiate DAGs
download_and_stage_to_mysql_dag = download_and_stage_to_mysql()
transform_and_load_to_postgres_dag = transform_and_load_to_postgres()
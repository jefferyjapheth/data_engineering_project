from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from kaggle.api.kaggle_api_extended import KaggleApi

logger = LoggingMixin().log

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["flight", "kaggle", "mysql", "ingestion"],
)
def data_ingestion():
    @task()
    def setup_data_dir():
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "mysql"))
        os.makedirs(data_dir, exist_ok=True)
        logger.info(f"Data directory set at {data_dir}")
        return data_dir

    @task()
    def download_dataset(data_dir: str, dataset: str):
        config_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
        os.environ["KAGGLE_CONFIG_DIR"] = config_dir
        os.makedirs(config_dir, exist_ok=True)  # avoid FileExistsError

        api = KaggleApi()
        api.authenticate()
        logger.info(f"Downloading dataset {dataset} to {data_dir}")
        api.dataset_download_files(dataset, path=data_dir, unzip=True)
        return data_dir

    @task()
    def find_csv(data_dir: str) -> str:
        for root, _, files in os.walk(data_dir):
            for file in files:
                if file.endswith(".csv"):
                    path = os.path.join(root, file)
                    logger.info(f"CSV found at {path}")
                    return path
        raise FileNotFoundError("CSV file not found in data directory")

    @task()
    def read_csv(csv_path: str):
        df = pd.read_csv(csv_path)
        logger.info(f"CSV loaded with {len(df)} rows")
        return df.to_json(orient="records")

    @task()
    def load_to_mysql(df_json: str, table_name: str):
        df = pd.read_json(df_json, orient="records")
        mysql_conn_str = Variable.get("mysql_conn_uri", default_var="mysql+mysqlconnector://mysql:amalitech@mysql/staging_db")
        engine = create_engine(mysql_conn_str)
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        logger.info(f"Data loaded into MySQL table: {table_name}")
        return True

    @task()
    def validate(df_json: str, table_name: str):
        df = pd.read_json(df_json, orient="records")
        mysql_conn_str = Variable.get("mysql_conn_uri", default_var="mysql+mysqlconnector://mysql:amalitech@mysql/staging_db")
        engine = create_engine(mysql_conn_str)
        inspector = inspect(engine)

        if table_name not in inspector.get_table_names():
            raise ValueError(f"Validation failed: Table '{table_name}' does not exist.")

        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count_db = result.scalar()

        if len(df) != row_count_db:
            raise ValueError(f"Row count mismatch: CSV={len(df)}, MySQL={row_count_db}")

        logger.info("Validation passed")
        return "Validation passed"

    dataset_name = "mahatiratusher/flight-price-dataset-of-bangladesh"
    table_name = "flight_prices"

    data_dir = setup_data_dir()
    downloaded_dir = download_dataset(data_dir, dataset_name)
    csv_path = find_csv(downloaded_dir)
    df_json = read_csv(csv_path)
    load_to_mysql(df_json, table_name)
    validate(df_json, table_name)

data_ingestion()

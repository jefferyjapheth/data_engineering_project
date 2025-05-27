from airflow.decorators import dag, task
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from kaggle.api.kaggle_api_extended import KaggleApi
from pandas.api.types import is_string_dtype, is_integer_dtype, is_float_dtype


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["flight", "kaggle", "mysql"],
)
def flight_price_ingestion():

    @task()
    def setup_data_dir():
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "mysql"))
        os.makedirs(data_dir, exist_ok=True)
        return data_dir

    @task()
    def download_dataset(data_dir: str, dataset: str):
        os.environ["KAGGLE_CONFIG_DIR"] = os.path.join(os.path.dirname(__file__), "..", "scripts")
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset, path=data_dir, unzip=True)
        return data_dir

    @task()
    def find_csv(data_dir: str) -> str:
        for root, _, files in os.walk(data_dir):
            for file in files:
                if file.endswith(".csv"):
                    return os.path.join(root, file)
        raise FileNotFoundError("CSV file not found in data directory")

    @task()
    def read_csv(csv_path: str):
        df = pd.read_csv(csv_path)
        return df.to_json(orient="records")  # Return as JSON string for XCom

    @task()
    def load_to_mysql(df_json: str, table_name: str):
        df = pd.read_json(df_json, orient="records")
        engine = create_engine("mysql+mysqlconnector://mysql:amalitech@mysql/staging_db")
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        return True

    @task()
    def validate(df_json: str, table_name: str):
        df = pd.read_json(df_json, orient="records")
        engine = create_engine("mysql+mysqlconnector://mysql:amalitech@mysql/staging_db")
        inspector = inspect(engine)

        if table_name not in inspector.get_table_names():
            raise ValueError(f"Validation failed: Table '{table_name}' does not exist.")

        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count_db = result.scalar()

        if len(df) != row_count_db:
            raise ValueError(f"Row count mismatch: CSV={len(df)}, MySQL={row_count_db}")

        expected_types = {
            "Airline": is_string_dtype,
            "Source": is_string_dtype,
            "Source Name": is_string_dtype,
            "Destination": is_string_dtype,
            "Destination Name": is_string_dtype,
            "Departure Date & Time": is_string_dtype,
            "Arrival Date & Time": is_string_dtype,
            "Duration (hrs)": is_float_dtype,
            "Stopovers": is_string_dtype,
            "Aircraft Type": is_string_dtype,
            "Class": is_string_dtype,
            "Booking Source": is_string_dtype,
            "Base Fare (BDT)": is_float_dtype,
            "Tax & Surcharge (BDT)": is_float_dtype,
            "Total Fare (BDT)": is_float_dtype,
            "Seasonality": is_string_dtype,
            "Days Before Departure": is_integer_dtype,
        }

        mismatches = {
            col: f"invalid type: {df[col].dtype}"
            for col, check_func in expected_types.items()
            if col in df and not check_func(df[col])
        }

        if mismatches:
            raise TypeError(f"Column type mismatches:\n{mismatches}")

        return "Validation passed"

    # DAG flow
    dataset_name = "mahatiratusher/flight-price-dataset-of-bangladesh"
    table_name = "flight_prices"

    data_dir = setup_data_dir()
    downloaded_dir = download_dataset(data_dir, dataset_name)
    csv_path = find_csv(downloaded_dir)
    df_json = read_csv(csv_path)
    load_to_mysql(df_json, table_name)
    validate(df_json, table_name)


flight_price_ingestion()

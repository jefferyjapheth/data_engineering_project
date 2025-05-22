import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from kaggle.api.kaggle_api_extended import KaggleApi

def setup_data_dir(base_dir=None):
    if base_dir is None:
        # Assuming this script lives in /opt/airflow/scripts inside container
        base_dir = os.path.dirname(os.path.abspath(__file__))
    # Adjust to match your mounted data directory inside container
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "mysql_data"))
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def download_kaggle_dataset(dataset, data_dir):
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path=data_dir, unzip=True)
    print(f"Dataset downloaded to: {data_dir}")

def find_csv_file(data_dir):
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                print("Found CSV:", csv_path)
                return csv_path
    raise FileNotFoundError("CSV file not found in the data directory.")

def read_csv(csv_path):
    df = pd.read_csv(csv_path)
    print("Data preview:")
    print(df.head())
    return df

def create_mysql_engine(user, password, host, db):
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

def load_data_to_mysql(df, engine, table_name):
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"Data loaded into MySQL table: {table_name}")

from pandas.api.types import (
    is_string_dtype,
    is_integer_dtype,
    is_float_dtype
)

def validate_ingestion(df, engine, table_name):
    inspector = inspect(engine)

    # Step 1: Check table exists
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Validation failed: Table '{table_name}' does not exist in the database.")

    # Step 2: Check row count matches
    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count_db = result.scalar()

    row_count_csv = len(df)
    if row_count_csv != row_count_db:
        raise ValueError(f"Row count mismatch: CSV={row_count_csv}, MySQL={row_count_db}")
    else:
        print(f"Row count validation passed: {row_count_csv} rows")

    # Step 3: Robust column type validation using pandas.api.types
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
        "Days Before Departure": is_integer_dtype
    }

    mismatches = {}
    for column, check_func in expected_types.items():
        if column not in df.columns:
            mismatches[column] = "missing"
        elif not check_func(df[column]):
            mismatches[column] = f"invalid type: {df[column].dtype}"

    if mismatches:
        raise TypeError(f"Column type mismatches detected:\n{mismatches}")
    else:
        print("Column data types validation passed")

    print("Data validation passed.")

def main(dataset, table_name):
    # Use env vars from docker-compose .env
    mysql_user = os.getenv("MYSQL_USER", "mysql_user")  # fallback to default
    mysql_password = os.getenv("MYSQL_PASSWORD", "Amalitech")
    mysql_host = "mysql"  # Docker Compose service name
    mysql_db = os.getenv("MYSQL_DATABASE", "staging_db")

    data_dir = setup_data_dir()
    download_kaggle_dataset(dataset, data_dir)
    csv_file = find_csv_file(data_dir)
    df = read_csv(csv_file)

    engine = create_mysql_engine(mysql_user, mysql_password, mysql_host, mysql_db)
    load_data_to_mysql(df, engine, table_name)
    validate_ingestion(df, engine, table_name)

if __name__ == "__main__":
    main(dataset="mahatiratusher/flight-price-dataset-of-bangladesh", table_name="flight_prices")

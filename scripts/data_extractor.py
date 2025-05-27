import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from kaggle.api.kaggle_api_extended import KaggleApi
from pandas.api.types import is_string_dtype, is_integer_dtype, is_float_dtype
from scripts.logger import get_logger

logger = get_logger("kaggle_data_loader")

def setup_data_dir(base_dir=None):
    try:
        base_dir = base_dir or os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "mysql"))
        os.makedirs(data_dir, exist_ok=True)
        logger.info(f"Data directory prepared at {data_dir}")
        return data_dir
    except Exception as e:
        logger.exception("Failed to create data directory.")
        raise

def download_kaggle_dataset(dataset, data_dir):
    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset, path=data_dir, unzip=True)
        logger.info(f"Dataset downloaded and unzipped to: {data_dir}")
    except Exception as e:
        logger.exception("Failed to download dataset from Kaggle.")
        raise

def find_csv_file(data_dir):
    try:
        for root, _, files in os.walk(data_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_path = os.path.join(root, file)
                    logger.info(f"Found CSV file: {csv_path}")
                    return csv_path
        raise FileNotFoundError("No CSV file found in the dataset.")
    except Exception as e:
        logger.exception("Failed to locate CSV file.")
        raise

def read_csv(csv_path):
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns.")
        return df
    except Exception as e:
        logger.exception("Failed to read CSV file.")
        raise

def create_mysql_engine(user, password, host, db):
    try:
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")
        logger.info(f"MySQL engine created for DB: {db}")
        return engine
    except Exception as e:
        logger.exception("Failed to create MySQL engine.")
        raise

def load_data_to_mysql(df, engine, table_name):
    try:
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        logger.info(f"Loaded data into MySQL table: {table_name}")
    except Exception as e:
        logger.exception(f"Failed to load data into MySQL table: {table_name}")
        raise

def validate_ingestion(df, engine, table_name):
    logger.info("Starting data validation...")
    inspector = inspect(engine)

    if table_name not in inspector.get_table_names():
        logger.error(f"Table '{table_name}' does not exist.")
        raise ValueError(f"Validation failed: Table '{table_name}' does not exist.")

    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count_db = result.scalar()

    row_count_csv = len(df)
    if row_count_csv != row_count_db:
        logger.error(f"Row count mismatch: CSV={row_count_csv}, MySQL={row_count_db}")
        raise ValueError("Row count mismatch between CSV and MySQL.")
    else:
        logger.info(f"Row count validation passed: {row_count_csv} rows.")

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
    for col, check in expected_types.items():
        if col not in df.columns:
            mismatches[col] = "missing"
        elif not check(df[col]):
            mismatches[col] = f"invalid type ({df[col].dtype})"

    if mismatches:
        logger.error(f"Data type mismatches found: {mismatches}")
        raise TypeError(f"Column type mismatches detected:\n{mismatches}")

    logger.info("Column data types validation passed.")
    logger.info("Data ingestion validated successfully.")

def main(dataset="mahatiratusher/flight-price-dataset-of-bangladesh", table_name="flight_prices"):
    try:
        logger.info("Starting data ingestion process...")

        mysql_user = os.getenv("MYSQL_USER", "mysql")
        mysql_password = os.getenv("MYSQL_PASSWORD", "amalitech")
        mysql_host = "mysql"
        mysql_db = os.getenv("MYSQL_DATABASE", "staging_db")

        data_dir = setup_data_dir()
        download_kaggle_dataset(dataset, data_dir)
        csv_file = find_csv_file(data_dir)
        df = read_csv(csv_file)

        engine = create_mysql_engine(mysql_user, mysql_password, mysql_host, mysql_db)
        load_data_to_mysql(df, engine, table_name)
        validate_ingestion(df, engine, table_name)

        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.error("ETL pipeline failed.")
        logger.exception(e)

if __name__ == "__main__":
    main()

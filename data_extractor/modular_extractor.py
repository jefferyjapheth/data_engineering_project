import os
import pandas as pd
from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi
from utils.logger import setup_logger, get_project_data_dir
from dotenv import load_dotenv

# Load env vars first
load_dotenv()

def setup_data_dir():
    data_dir = get_project_data_dir()
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def download_kaggle_dataset(dataset, data_dir, logger):
    api = KaggleApi()
    api.authenticate()
    logger.info(f"Starting dataset download: {dataset}")
    api.dataset_download_files(dataset, path=data_dir, unzip=True)
    logger.info(f"Dataset downloaded to: {data_dir}")

def find_csv_file(data_dir, logger):
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                logger.info(f"Found CSV file: {csv_path}")
                return csv_path
    logger.error("CSV file not found in the data directory.")
    raise FileNotFoundError("CSV file not found in the data directory.")

def read_csv(csv_path, logger):
    logger.info(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"CSV data preview:\n{df.head()}")
    return df

def create_mysql_engine(user, password, host, db, logger):
    logger.info(f"Creating MySQL engine for database '{db}' on host '{host}'")
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

def load_data_to_mysql(df, engine, table_name, logger):
    logger.info(f"Loading data into MySQL table: {table_name}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logger.info(f"Data successfully loaded into MySQL table: {table_name}")

def main(dataset, table_name):
    data_dir = setup_data_dir()
    log_path = os.path.join(data_dir, "logs", "etl.log")
    logger = setup_logger("data_extractor", log_file=log_path)

    download_kaggle_dataset(dataset, data_dir, logger)
    csv_file = find_csv_file(data_dir, logger)
    df = read_csv(csv_file, logger)

    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_host = os.getenv("MYSQL_HOST", "mysql")
    mysql_db = os.getenv("MYSQL_DATABASE")

    if not all([mysql_user, mysql_password, mysql_db]):
        logger.error("Missing required MySQL environment variables: MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")
        raise ValueError("Missing required MySQL environment variables.")

    engine = create_mysql_engine(mysql_user, mysql_password, mysql_host, mysql_db, logger)
    load_data_to_mysql(df, engine, table_name, logger)

if __name__ == "__main__":
    main(dataset="mahatiratusher/flight-price-dataset-of-bangladesh", table_name="flight_prices")




import os
import pandas as pd
from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi
from utils.logger import setup_logger, get_project_data_dir
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup data directory and logger
data_dir = get_project_data_dir()
log_path = os.path.join(data_dir, "logs", "etl.log")
logger = setup_logger("data_extractor", log_file=log_path)

def setup_data_dir():
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Data directory setup at {data_dir}")
    return data_dir

def download_kaggle_dataset(dataset, data_dir):
    api = KaggleApi()
    api.authenticate()
    logger.info(f"Starting dataset download: {dataset}")
    api.dataset_download_files(dataset, path=data_dir, unzip=True)
    logger.info(f"Dataset downloaded to: {data_dir}")

def find_csv_file(data_dir):
    for root, _, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                logger.info(f"Found CSV file: {csv_path}")
                return csv_path
    logger.error("CSV file not found in the data directory.")
    raise FileNotFoundError("CSV file not found in the data directory.")

def read_csv(csv_path):
    logger.info(f"Reading CSV file: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"CSV data preview:\n{df.head()}")
    return df

def create_mysql_engine(user, password, host, db):
    logger.info(f"Creating MySQL engine for database '{db}' on host '{host}'")
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

def load_data_to_mysql(df, engine, table_name):
    logger.info(f"Loading data into MySQL table: {table_name}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logger.info(f"Data successfully loaded into MySQL table: {table_name}")

def main(dataset, table_name):
    setup_data_dir()
    download_kaggle_dataset(dataset, data_dir)
    csv_file = find_csv_file(data_dir)
    df = read_csv(csv_file)

    mysql_user = os.getenv("MYSQL_USER")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_host = os.getenv("MYSQL_HOST", "mysql")  # default to 'mysql' if unset
    mysql_db = os.getenv("MYSQL_DATABASE")

    if not all([mysql_user, mysql_password, mysql_db]):
        raise ValueError("Missing required MySQL environment variables: MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE")

    engine = create_mysql_engine(mysql_user, mysql_password, mysql_host, mysql_db)
    load_data_to_mysql(df, engine, table_name)

if __name__ == "__main__":
    main(dataset="mahatiratusher/flight-price-dataset-of-bangladesh", table_name="flight_prices")

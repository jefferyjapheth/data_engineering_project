import os
import pandas as pd
from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi

def setup_data_dir(base_dir=None):
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))
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

def main(dataset, table_name):
    data_dir = setup_data_dir()
    download_kaggle_dataset(dataset, data_dir)
    csv_file = find_csv_file(data_dir)
    df = read_csv(csv_file)

    mysql_user = os.environ["MYSQL_USER"]
    mysql_password = os.environ["MYSQL_PASSWORD"]
    mysql_host = "mysql"
    mysql_db = os.environ["MYSQL_DATABASE"]

    engine = create_mysql_engine(mysql_user, mysql_password, mysql_host, mysql_db)
    load_data_to_mysql(df, engine, table_name)

if __name__ == "__main__":
    # Example usage, replace table_name as needed
    main(dataset="mahatiratusher/flight-price-dataset-of-bangladesh", table_name="flight_prices")

import os
from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

data_dir = "../data"  # This will be /app/data inside container

os.makedirs(data_dir, exist_ok=True)

dataset = "mahatiratusher/flight-price-dataset-of-bangladesh"
api.dataset_download_files(dataset, path=data_dir, unzip=True)

print(f"Dataset downloaded to: {data_dir}")

for root, dirs, files in os.walk(data_dir):
    for file in files:
        print("Found file:", os.path.join(root, file))

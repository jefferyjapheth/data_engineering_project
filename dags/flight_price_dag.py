from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys

# Add script path to import the data_extractor module
sys.path.append('/opt/airflow/scripts')

from data_extractor import main as extract_data  # Assumes a main() function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or later define a schedule
    catchup=False,
    description='Extract flight data, load to MySQL, transform with Spark, and load to Postgres',
) as dag:

    extract_task = PythonOperator(
        task_id='download_and_load_to_mysql',
        python_callable=extract_data
    )

    transform_task = SparkSubmitOperator(
        task_id='transform_and_load_to_postgres',
        application='/opt/airflow/scripts/spark_transform.py',
        conn_id='spark_default',
        jars='/opt/airflow/connectors/mysql-connector-j-9.3.0.jar,/opt/airflow/connectors/postgresql-42.7.5.jar',
        verbose=True
    )

    extract_task >> transform_task

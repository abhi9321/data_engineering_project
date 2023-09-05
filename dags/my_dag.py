from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.spark.spark_session import create_spark_session
from src.data_pipeline.extract.extract_data import extract_data
from src.data_pipeline.transform.transform_data import transform_data
from src.data_pipeline.load.load_data import load_data
from config.data_config import DATA_PATH, RAW_DATA_DIR, INTERMEDIATE_DATA_DIR, FINAL_DATA_DIR
from utils.common_utlis import create_directory_if_not_exists

# Define your DAG
dag = DAG(
    "my_data_engineering_dag",
    schedule_interval=None,  # You can specify the scheduling interval here
    start_date=datetime(2023, 9, 1),  # Adjust the start date
    catchup=False,
)


# Task to run the ETL process
def run_etl():
    # Create required directories
    raw_data_path = f"{DATA_PATH}{RAW_DATA_DIR}"
    intermediate_data_path = f"{DATA_PATH}{INTERMEDIATE_DATA_DIR}"
    final_data_path = f"{DATA_PATH}{FINAL_DATA_DIR}"

    create_directory_if_not_exists(raw_data_path)
    create_directory_if_not_exists(intermediate_data_path)
    create_directory_if_not_exists(final_data_path)

    spark = create_spark_session()
    input_path = f"{raw_data_path}input.csv"
    output_path = f"{final_data_path}output.csv"

    data = extract_data(spark, input_path)
    transformed_data = transform_data(data)
    load_data(transformed_data, output_path)


etl_task = PythonOperator(
    task_id="run_etl",
    python_callable=run_etl,
    dag=dag,
)

etl_task

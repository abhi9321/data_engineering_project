from src.spark.spark_session import create_spark_session
from src.data_pipeline.transform.transform_data import transform_data


def extract_data(spark, input_path):
    df = spark.read.csv(input_path, header=True)
    return df

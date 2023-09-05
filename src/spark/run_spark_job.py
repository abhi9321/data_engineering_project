import os
from spark_session import create_spark_session
from src.data_pipeline.extract.extract_data import extract_data
from src.data_pipeline.transform.transform_data import transform_data
from src.data_pipeline.load.load_data import load_data

os.environ['HADOOP_HOME'] = r'C:\Users\4906031\Documents\abhishek\Program Files\hadoop-2.6.1'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '/bin:' + os.environ['PATH']


def main():
    # Get the absolute path to the project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

    # Create Spark Session
    spark = create_spark_session()

    # Define file paths
    input_path = os.path.join(project_root, 'data', 'input', 'input.csv')
    output_path = os.path.join(project_root, 'data', 'final', 'output.csv')

    # Extract, transform and load data using Spark
    data = extract_data(spark, input_path)
    clean_data = transform_data(data)
    load_data(clean_data, output_path)


if __name__ == "__main__":
    main()

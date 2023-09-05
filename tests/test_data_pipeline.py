# Sample unit tests for your data pipeline
import unittest
from src.data_pipeline.extract.extract_data import extract_data
from src.data_pipeline.transform.transform_data import transform_data


class TestDataPipeline(unittest.TestCase):
    def test_extract_data(self):
        # Add your test code here for data extraction
        pass

    def test_transform_data(self):
        # Add your test code here for data transformation
        pass


if __name__ == "__main__":
    unittest.main()

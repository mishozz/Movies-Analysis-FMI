import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import unittest
from unittest.mock import patch, MagicMock, mock_open
from dags.persistence.dataframe_repository import DataFrameRepository

class TestDataFrameRepository(unittest.TestCase):

    @patch('dags.spark.spark_manager.SparkSessionManager.get_session')
    @patch('os.makedirs')
    @patch('os.path.exists')
    def setUp(self, mock_exists, mock_makedirs, mock_get_session):
        mock_exists.return_value = False
        self.mock_spark = MagicMock()
        mock_get_session.return_value = self.mock_spark
        self.repo = DataFrameRepository(parquet_dir="test_parquet_store")

    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_connect(self, mock_makedirs, mock_exists):
        mock_exists.return_value = False
        self.repo.connect()
        mock_makedirs.assert_called_once_with("test_parquet_store")

    @patch('os.path.exists')
    @patch('dags.persistence.dataframe_repository.DataFrameRepository._save_object')
    def test_save_figure(self, mock_save_object, mock_exists):
        fig = MagicMock()
        self.repo.save_figure("test_figure", fig)
        mock_save_object.assert_called_once_with("fig_test_figure", fig)

    @patch('os.path.exists')
    @patch('dags.persistence.dataframe_repository.DataFrameRepository._load_object')
    def test_load_figure(self, mock_load_object, mock_exists):
        mock_load_object.return_value = "figure"
        result = self.repo.load_figure("test_figure")
        mock_load_object.assert_called_once_with("fig_test_figure")
        self.assertEqual(result, "figure")

    @patch('os.path.exists')
    @patch('dags.persistence.dataframe_repository.DataFrameRepository._save_object')
    def test_save_data(self, mock_save_object, mock_exists):
        data = {"key": "value"}
        self.repo.save_data("test_data", data)
        mock_save_object.assert_called_once_with("test_data", data)

    @patch('os.path.exists')
    @patch('dags.persistence.dataframe_repository.DataFrameRepository._load_object')
    def test_load_data(self, mock_load_object, mock_exists):
        mock_load_object.return_value = {"key": "value"}
        result = self.repo.load_data("test_data")
        mock_load_object.assert_called_once_with("test_data")
        self.assertEqual(result, {"key": "value"})

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('pickle.dump')
    def test_save_object(self, mock_pickle_dump, mock_open, mock_exists):
        mock_exists.return_value = True
        obj = {"key": "value"}
        self.repo._save_object("test_object", obj)
        mock_open.assert_called_once_with(os.path.join("test_parquet_store", "test_object.pkl"), "wb")
        mock_pickle_dump.assert_called_once_with(obj, mock_open())

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    @patch('pickle.load')
    def test_load_object(self, mock_pickle_load, mock_open, mock_exists):
        mock_exists.return_value = True
        mock_pickle_load.return_value = {"key": "value"}
        result = self.repo._load_object("test_object")
        mock_open.assert_called_once_with(os.path.join("test_parquet_store", "test_object.pkl"), "rb")
        mock_pickle_load.assert_called_once_with(mock_open())
        self.assertEqual(result, {"key": "value"})

    @patch('os.path.exists')
    @patch('dags.persistence.dataframe_repository.DataFrameRepository._save_object')
    def test_save_dataframe(self, mock_save_object, mock_exists):
        mock_exists.return_value = True
        df = MagicMock()
        self.repo.save_dataframe("test_dataframe", df)
        df.write.mode().parquet.assert_called_once_with(os.path.join("test_parquet_store", "test_dataframe.parquet"))

    @patch('os.path.exists')
    def test_load_dataframe(self, mock_exists):
        mock_exists.return_value = True
        self.repo.load_dataframe("test_dataframe")
        self.mock_spark.read.parquet.assert_called_once_with(os.path.join("test_parquet_store", "test_dataframe.parquet"))

if __name__ == '__main__':
    unittest.main()

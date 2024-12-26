import os
import pickle
from typing import Any
from dags.persistence.base_repository_interface import BaseRepositoryInterface
from spark.sparkManager import SparkSessionManager

class DataFrameRepository(BaseRepositoryInterface):
    def __init__(self, parquet_dir: str = "parquet_store"):
        self.parquet_dir = parquet_dir
        self.spark = SparkSessionManager.get_session()
        self.connect()

    def connect(self):
        if not os.path.exists(self.parquet_dir):
            os.makedirs(self.parquet_dir)

    def save_dataframe(self, name: str, df: Any):
        file_path = os.path.join(self.parquet_dir, f"{name}.parquet")
        df.write.mode("overwrite").parquet(file_path)

    def load_dataframe(self, name: str) -> Any:
        file_path = os.path.join(self.parquet_dir, f"{name}.parquet")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No Parquet file found for table '{name}'")
        return self.spark.read.parquet(file_path)

    def save_figure(self, name: str, fig: Any):
        self._save_object(f"fig_{name}", fig)

    def load_figure(self, name: str) -> Any:
        return self._load_object(f"fig_{name}")

    def save_data(self, name: str, data: Any):
        self._save_object(name, data)

    def load_data(self, name: str) -> Any:
        return self._load_object(name)

    def _save_object(self, name: str, obj: Any):
        file_path = os.path.join(self.parquet_dir, f"{name}.pkl")
        with open(file_path, "wb") as f:
            pickle.dump(obj, f)

    def _load_object(self, name: str) -> Any:
        file_path = os.path.join(self.parquet_dir, f"{name}.pkl")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No pickle file found for '{name}'")
        with open(file_path, "rb") as f:
            return pickle.load(f)

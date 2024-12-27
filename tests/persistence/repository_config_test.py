import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import unittest
from dags.persistence.repository_config import RepositoryConfig, PROD_ENV
from dags.persistence.dataframe_repository import DataFrameRepository

class TestRepositoryConfig(unittest.TestCase):
    def test_get_repository_instance_dev(self):
        repository_instance = RepositoryConfig.get_repository_instance()
        self.assertIsInstance(repository_instance, DataFrameRepository)

    def test_get_repository_instance_prod(self):
        with self.assertRaises(NotImplementedError):
            RepositoryConfig._get_repository(env=PROD_ENV)

    def test_get_repository_instance_unknown_env(self):
        with self.assertRaises(RuntimeError):
            RepositoryConfig._get_repository(env="UNKNOWN_ENV")

if __name__ == '__main__':
    unittest.main()

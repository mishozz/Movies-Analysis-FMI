import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import unittest
from unittest.mock import patch
from dags.persistence.repository_config import RepositoryConfig, PROD_ENV
from dags.persistence.dataframe_repository import DataFrameRepository

class TestRepositoryConfig(unittest.TestCase):
    def setUp(self):
        RepositoryConfig._repository = None

    def test_get_repository_instance_dev(self):
        with patch('dags.persistence.dataframe_repository.DataFrameRepository') as MockDataFrameRepository:
            mock_instance = MockDataFrameRepository.return_value
            repository_instance = RepositoryConfig.get_repository_instance()
            self.assertIsInstance(repository_instance, DataFrameRepository)
            self.assertEqual(repository_instance, mock_instance)

    def test_get_repository_instance_cached(self):
        with patch('dags.persistence.dataframe_repository.DataFrameRepository') as MockDataFrameRepository:
            repository_instance_1 = RepositoryConfig.get_repository_instance()
            repository_instance_2 = RepositoryConfig.get_repository_instance()
            self.assertIs(repository_instance_1, repository_instance_2)
            MockDataFrameRepository.assert_called_once()

    def test_get_repository_instance_prod(self):
        with self.assertRaises(NotImplementedError):
            RepositoryConfig._get_repository(env=PROD_ENV)

    def test_get_repository_instance_unknown_env(self):
        with self.assertRaises(RuntimeError):
            RepositoryConfig._get_repository(env="UNKNOWN_ENV")

if __name__ == '__main__':
    unittest.main()
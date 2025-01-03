import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import unittest
from unittest.mock import MagicMock
from airflow import DAG
from datetime import datetime
from dags.airflow_operators.operators import DataPipelineOperator

class TestDataPipelineOperator(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(dag_id='test_dag', start_date=datetime(2024, 1, 1))
        self.operator = DataPipelineOperator(self.dag)

    def test_create_operator(self):
        task_id = 'test_task'
        python_callable = MagicMock()
        operator = self.operator.create_operator(task_id, python_callable)
        
        self.assertEqual(operator.task_id, task_id)
        self.assertEqual(operator.python_callable, python_callable)
        self.assertEqual(operator.dag, self.dag)

    def test_load_data(self):
        operator = self.operator.load_data()
        self.assertEqual(operator.task_id, 'load_data')
        self.assertEqual(operator.python_callable.__name__, 'load_data')
        self.assertEqual(operator.dag, self.dag)

    def test_transform_data(self):
        operator = self.operator.transform_data()
        self.assertEqual(operator.task_id, 'transform_data')
        self.assertEqual(operator.python_callable.__name__, 'transform_data')
        self.assertEqual(operator.dag, self.dag)

    def test_analyze_trends(self):
        operator = self.operator.analyze_trends()
        self.assertEqual(operator.task_id, 'analyze_trends')
        self.assertEqual(operator.python_callable.__name__, 'analyze_trends')
        self.assertEqual(operator.dag, self.dag)

    def test_analyze_genres(self):
        operator = self.operator.analyze_genres()
        self.assertEqual(operator.task_id, 'analyze_genres')
        self.assertEqual(operator.python_callable.__name__, 'analyze_genres')
        self.assertEqual(operator.dag, self.dag)

    def test_analyze_titles(self):
        operator = self.operator.analyze_titles()
        self.assertEqual(operator.task_id, 'analyze_titles')
        self.assertEqual(operator.python_callable.__name__, 'analyze_titles')
        self.assertEqual(operator.dag, self.dag)

    def test_save_report(self):
        operator = self.operator.save_report()
        self.assertEqual(operator.task_id, 'save_report')
        self.assertEqual(operator.python_callable.__name__, 'save_report')
        self.assertEqual(operator.dag, self.dag)

    def test_analyze_genres_by_count(self):
        operator = self.operator.analyze_genres_by_count()
        self.assertEqual(operator.task_id, 'analyze_genres_by_titles_count')
        self.assertEqual(operator.python_callable.__name__, 'analyze_genres_by_count')
        self.assertEqual(operator.dag, self.dag)

    def test_analyze_actors(self):
        operator = self.operator.analyze_actors()
        self.assertEqual(operator.task_id, 'analyze_movie_actors_with_highest_ratings')
        self.assertEqual(operator.python_callable.__name__, 'analyze_actors')
        self.assertEqual(operator.dag, self.dag)

if __name__ == '__main__':
    unittest.main()

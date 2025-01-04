from dags.airflow_tasks.tasks_manager import TasksManager
from airflow.operators.python import PythonOperator
from dags.spark.spark_manager import SparkSessionManager
from dags.persistence.repository_config import RepositoryConfig
from dags.data_utils.data_utils import DataUtils

class DataPipelineOperator:
    def __init__(self, dag):
        self.dag = dag
        df_repo = RepositoryConfig.get_repository_instance()
        spark = SparkSessionManager.get_session()
        data_utils = DataUtils()
        self.tasks_manager = TasksManager(df_repo=df_repo, spark=spark, data_utils=data_utils)

    def create_operator(self, task_id, python_callable):
        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            dag=self.dag
        )

    def load_data(self):
        return self.create_operator('load_data', self.tasks_manager.load_data)

    def transform_data(self):
        return self.create_operator('transform_data', self.tasks_manager.transform_data)

    def analyze_trends(self):
        return self.create_operator('analyze_trends', self.tasks_manager.analyze_trends)

    def analyze_genres(self):
        return self.create_operator('analyze_genres', self.tasks_manager.analyze_genres)

    def analyze_titles(self):
        return self.create_operator('analyze_titles', self.tasks_manager.analyze_titles)
    
    def analyze_actors(self):
        return self.create_operator('analyze_movie_actors_with_highest_ratings', self.tasks_manager.analyze_actors)
    
    def analyze_genres_by_count(self):
        return self.create_operator('analyze_genres_by_titles_count', self.tasks_manager.analyze_genres_by_count)
    
    def analyze_titles_by_count(self):
        return self.create_operator('analyze_titles_by_count', self.tasks_manager.analyze_titles_count_by_type)
    
    def analyze_most_productive_actors(self):
        return self.create_operator('analyze_most_productive_actors', self.tasks_manager.analyze_most_productive_actors)

    def save_report(self):
        return self.create_operator('save_report', self.tasks_manager.save_report)

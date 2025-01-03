from dags.airflow_tasks.tasks import load_data, transform_data, analyze_trends, analyze_genres, analyze_titles, save_report, analyze_actors, analyze_genres_by_count
from airflow.operators.python import PythonOperator

class DataPipelineOperator:
    def __init__(self, dag):
        self.dag = dag

    def create_operator(self, task_id, python_callable):
        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            dag=self.dag
        )

    def load_data(self):
        return self.create_operator('load_data', load_data)

    def transform_data(self):
        return self.create_operator('transform_data', transform_data)

    def analyze_trends(self):
        return self.create_operator('analyze_trends', analyze_trends)

    def analyze_genres(self):
        return self.create_operator('analyze_genres', analyze_genres)

    def analyze_titles(self):
        return self.create_operator('analyze_titles', analyze_titles)
    
    def analyze_actors(self):
        return self.create_operator('analyze_movie_actors_with_highest_ratings', analyze_actors)
    
    def analyze_genres_by_count(self):
        return self.create_operator('analyze_genres_by_titles_count', analyze_genres_by_count)

    def save_report(self):
        return self.create_operator('save_report', save_report)

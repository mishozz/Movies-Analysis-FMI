from airflow_operators.operators import DataPipelineOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'mzlatev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag_id = 'imdb_analysis'

def create_dag(dag_id, default_args):
    dag = DAG(
        dag_id,
        default_args=default_args,
        description='IMDb data analysis pipeline',
        catchup=False
    )

    with dag:
        operator = DataPipelineOperator(dag)
        operator.load_data() >> operator.transform_data() >> [operator.analyze_trends(), operator.analyze_genres(), operator.analyze_titles(), operator.analyze_actors(), operator.analyze_genres_by_count()] >> operator.save_report()

    return dag

globals()[dag_id] = create_dag(dag_id, default_args)

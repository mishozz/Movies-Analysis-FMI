from airflow_operators.operators import load_data_operator, analyze_trends_operator, analyze_genres_operator, analyze_titles_operator, save_report_operator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
'imdb_analysis',
default_args=default_args,
description='IMDb data analysis pipeline',
schedule_interval=timedelta(days=1),
catchup=False
)

load_data_operator(dag) >> [analyze_trends_operator(dag), analyze_genres_operator(dag), analyze_titles_operator(dag)] >> save_report_operator(dag)

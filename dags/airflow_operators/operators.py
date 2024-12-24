from airflow_tasks.tasks import load_data, analyze_trends, analyze_genres, analyze_titles, save_report
from airflow.operators.python import PythonOperator

def load_data_operator(dag):
    return PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
    )
    
def analyze_trends_operator(dag):
    return PythonOperator(
        task_id='analyze_trends',
        python_callable=analyze_trends,
        dag=dag
    )
    
def analyze_genres_operator(dag):
    return PythonOperator(
        task_id='analyze_genres',
        python_callable=analyze_genres,
        dag=dag
    )

def analyze_titles_operator(dag):
    return PythonOperator(
        task_id='analyze_titles',
        python_callable=analyze_titles,
        dag=dag
    )

def save_report_operator(dag):
    return PythonOperator(
        task_id='save_report',
        python_callable=save_report,
        dag=dag
   )    


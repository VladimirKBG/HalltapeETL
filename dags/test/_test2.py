from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_simple2", 
    start_date=datetime(2025,1,1), 
    schedule=None,
    tags=["test_del"]
):
    BashOperator(task_id="test2",
                 bash_command="echo hi")
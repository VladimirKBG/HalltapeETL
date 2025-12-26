import textwrap
import pendulum
import pandas as pd
import os, sys

from pathlib import Path
from airflow.sdk import dag, task

SCRIPTS_ROOT = "/data/scripts"
if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)
from create_data import update_data


default_args = {
    "depends_on_past": True,
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=15),
}

@dag(
    default_args=default_args,
    description="Update data in data/csv directory.",
    schedule="0,5,10,15,20,25,30,35,40,45,50,55 * * * *",
    start_date=pendulum.datetime(2025, 12, 25, 12, 00),
    catchup=False,
    tags=["source", "csv"], 
)
def update_data__csv():
    path = get_path_to_csv_data()
    update_csv_data(path)


@task
def get_path_to_csv_data():
    return os.getenv("CSV_DATA_PATH", "")


@task
def update_csv_data(path):
    update_data(
        path,
    )
    
update_data__csv()

import pendulum
import sys

from airflow.sdk import dag, task
from data_source.update_data__csv import get_path_to_csv_data

SCRIPTS_ROOT = "/data/scripts"
if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)
from create_data import generate_initial_data


@dag(
    description="Generate new csv files, or rewrite its.",
    start_date=pendulum.now(),
    schedule=None,
    tags=["source", "csv"], 
)
def generate_data__csv():
    path = get_path_to_csv_data.override(task_id="get_path_to_truncated_files")()
    generate_csv_data(path) # type: ignore


@task()
def generate_csv_data(path):
    generate_initial_data(path)


generate_data__csv()

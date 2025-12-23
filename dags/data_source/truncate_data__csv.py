import pendulum
import sys

from airflow.sdk import dag, task
from update_data__csv import get_path_to_csv_data

SCRIPTS_ROOT = "/data/scripts"
if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)
from create_data import truncate_data_files


@dag(
    description="Truncate data in existings csv data-files, preserve headers.",
    start_date=pendulum.now(),
    schedule=None,
    tags=["source", "csv"], 
)
def truncate_data__csv():
    path = get_path_to_csv_data.override(task_id="get_path_to_truncated_files")()
    truncate_csv_data(path)


@task()
def truncate_csv_data(path):
    truncate_data_files(path)

truncate_data__csv()

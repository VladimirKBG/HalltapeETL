import pendulum
import sys

from airflow.sdk import dag, task
from update_data__csv import get_path_to_csv_data
from truncate_data__csv import truncate_csv_data
from generate_data__csv import generate_csv_data

SCRIPTS_ROOT = "/data/scripts"
if SCRIPTS_ROOT not in sys.path:
    sys.path.insert(0, SCRIPTS_ROOT)
from create_data import generate_initial_data


@dag(
    description="Rewrites existings csv data-files.",
    start_date=pendulum.now(),
    schedule=None,
    tags=["source", "csv"], 
)
def refresh_data__csv():
    path = get_path_to_csv_data.override(task_id="get_path_to_truncated_files")()
    (truncate_csv_data.override(task_id="truncate_csv_data_before_refresh")(path) 
        >> generate_csv_data.override(task_id="generate_data_after_truncating")(path)) # type: ignore


refresh_data__csv()
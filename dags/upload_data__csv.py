import pendulum
import csv

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


SCHEMA = "raw"
SQLs = {
    "categories": "/sql/ddl/categories.sql",
    "clients": "/sql/ddl/clients.sql",
    "products": "/sql/ddl/products.sql",
    "orders": "/sql/ddl/orders.sql",
    "order_items": "/sql/ddl/order_items.sql",
}
TABLES = {
    "categories": "dim_categories",
    "clients": "dim_clients",
    "products": "dim_products",
    "orders": "fct_orders",
    "order_items": "fct_order_items",
}

default_args = {
    "depends_on_past": False,
    "retries": 3,
    "rerty_delay": pendulum.duration(seconds=15),
}

@dag(
    start_date=pendulum.datetime(2025, 12, 25, 12, 00),
    schedule="0,10,20,30,40,50 * * * *",
    template_searchpath="/sql",
    default_args=default_args,
    description="Create tables, upload data from csv files.",
    tags=["upload", "csv"],
    params={
        "tables": TABLES,
        "sqls": SQLs,
        "schema": SCHEMA,
    }
)
def upload_data__csv():
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for__update_data__csv",
        external_dag_id="update_data__csv",
        mode="reschedule",
        #execution_date_fn=lambda dt, **ctx: dt,
        allowed_states=["success"],
        poke_interval=pendulum.duration(seconds=60),
        timeout = 150,
    )
    upload_categories = SQLExecuteQueryOperator(
        task_id="upload_categories",
        sql="/sql/ddl/categories.sql",
        conn_id="airflow",
        retries=0,
    )
    upload_clients = SQLExecuteQueryOperator(
        task_id="upload_clients",
        sql="/sql/ddl/clients.sql",
        conn_id="airflow",
        retries=0,
    )
    upload_products = SQLExecuteQueryOperator(
        task_id="upload_products",
        sql="/sql/ddl/products.sql",
        conn_id="airflow",
        retries=0,
    )
    upload_orders = SQLExecuteQueryOperator(
        task_id="upload_orders",
        sql="/sql/ddl/orders.sql",
        conn_id="airflow",
        retries=0,
    )
    upload_order_items = SQLExecuteQueryOperator(
        task_id="upload_order_items",
        sql="/sql/ddl/order_items.sql",
        conn_id="airflow",
        retries=0,
    )
    cnt_categories = SQLExecuteQueryOperator(
        task_id="cnt_categories",
        sql="/sql/dml/categories.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_clients = SQLExecuteQueryOperator(
        task_id="cnt_clients",
        sql="/sql/dml/clients.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_products = SQLExecuteQueryOperator(
        task_id="cnt_products",
        sql="/sql/dml/products.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_orders = SQLExecuteQueryOperator(
        task_id="cnt_orders",
        sql="/sql/dml/orders.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_order_items = SQLExecuteQueryOperator(
        task_id="cnt_order_items",
        sql="/sql/dml/order_items.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    trigger_notifyier = TriggerDagRunOperator(
        task_id="trigger_notifyier",
        trigger_dag_id="notify_tg",
        conf={"message": "CSV data successfully uploaded", "logical_date": "{{ logical_date }}",},
        wait_for_completion=False,
    )
    trigger_notifyer_on_failure = TriggerDagRunOperator(
        task_id="notify_on_failure",
        trigger_dag_id="notify_tg",
        conf={"message": "DAG id={{ dag.dag_id }} FAILED in run id={{ run_id }}", "logical_date": "{{ logical_date }}"},
        wait_for_completion=False,
        trigger_rule="one_failed"
    )

    wait_for_upstream >> [upload_categories, upload_clients, upload_products, upload_orders, upload_order_items] #type: ignore
    upload_categories >> upload_products #type: ignore
    upload_clients >> upload_orders #type: ignore
    [upload_products, upload_orders]  >> upload_order_items #type: ignore
    upload_categories >> cnt_categories #type: ignore
    upload_clients >> cnt_clients #type: ignore
    upload_products >> cnt_products #type: ignore
    upload_orders >> cnt_orders #type: ignore
    upload_order_items >> cnt_order_items #type: ignore
    [check_rows_count(cnt_categories.output), cnt_order_items, cnt_orders, cnt_products] >> trigger_notifyier #type: ignore
    trigger_notifyier >> trigger_notifyer_on_failure #type: ignore

@task(
    retry_delay=pendulum.duration(seconds=1),
)
def check_rows_count(cnt_cat):
    cnt_in_sql = cnt_cat[0][0]
    with open("/data/csv/categories.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")

upload_data__csv()

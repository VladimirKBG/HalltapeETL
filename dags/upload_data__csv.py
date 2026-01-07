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
    schedule="0 */10 * * * * *",
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
        sql="/sql/dml/categories_cnt.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_clients = SQLExecuteQueryOperator(
        task_id="cnt_clients",
        sql="/sql/dml/clients_cnt.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_products = SQLExecuteQueryOperator(
        task_id="cnt_products",
        sql="/sql/dml/products_cnt.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_orders = SQLExecuteQueryOperator(
        task_id="cnt_orders",
        sql="/sql/dml/orders_cnt.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    cnt_order_items = SQLExecuteQueryOperator(
        task_id="cnt_order_items",
        sql="/sql/dml/order_items_cnt.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )

    check_data = check_rows_count(
        categories=cnt_categories.output,
        clients=cnt_clients.output,
        products=cnt_products.output,
        orders=cnt_orders.output,
        order_items=cnt_order_items.output,
    )

    update_categories = SQLExecuteQueryOperator(
        task_id="update_categories",
        sql="/sql/dml/categories_update.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    update_clients = SQLExecuteQueryOperator(
        task_id="update_clients",
        sql="/sql/dml/clients_update.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    update_products = SQLExecuteQueryOperator(
        task_id="update_products",
        sql="/sql/dml/products_update.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    update_orders = SQLExecuteQueryOperator(
        task_id="update_orders",
        sql="/sql/dml/orders_update.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    update_order_items = SQLExecuteQueryOperator(
        task_id="update_order_items",
        sql="/sql/dml/order_items_update.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )

    delete_temp_categories = SQLExecuteQueryOperator(
        task_id="delete_temp_categories",
        sql="/sql/dml/categories_delete_temp.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    delete_temp_clients = SQLExecuteQueryOperator(
        task_id="delete_temp_clients",
        sql="/sql/dml/clients_delete_temp.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    delete_temp_products = SQLExecuteQueryOperator(
        task_id="delete_temp_products",
        sql="/sql/dml/products_delete_temp.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    delete_temp_orders = SQLExecuteQueryOperator(
        task_id="delete_temp_orders",
        sql="/sql/dml/orders_delete_temp.sql",
        conn_id="airflow",
        retry_delay=pendulum.duration(seconds=1),
    )
    delete_temp_order_items = SQLExecuteQueryOperator(
        task_id="delete_temp_order_items",
        sql="/sql/dml/order_items_delete_temp.sql",
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
    check_data >> [update_categories, update_clients] #type: ignore
    update_categories >> update_products >> update_order_items #type: ignore
    update_clients >> update_orders >> update_order_items >> [
        delete_temp_categories, delete_temp_clients, delete_temp_products, delete_temp_orders, delete_temp_order_items
    ] >> trigger_notifyier >> trigger_notifyer_on_failure #type: ignore

@task(
    retry_delay=pendulum.duration(seconds=1),
)
def check_rows_count(**kwargs):
    cnt_in_sql = kwargs["categories"][0][0]
    with open("/data/csv/categories.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")
    cnt_in_sql = kwargs["clients"][0][0]
    with open("/data/csv/clients.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")
    cnt_in_sql = kwargs["products"][0][0]
    with open("/data/csv/products.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")
    cnt_in_sql = kwargs["orders"][0][0]
    with open("/data/csv/orders.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")
    cnt_in_sql = kwargs["order_items"][0][0]
    with open("/data/csv/order_items.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")

upload_data__csv()

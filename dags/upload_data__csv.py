import pendulum
import csv

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


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

@dag(
    start_date=pendulum.datetime(2025, 12, 23),
    schedule=pendulum.duration(minutes=30),
    template_searchpath="/sql",
    description="Create tables, upload data from csv files.",
    tags=["upload", "csv"],
    params={
        "tables": TABLES,
        "sqls": SQLs,
        "schema": SCHEMA,
    }
)
def upload_data__csv():
    upload_categories = SQLExecuteQueryOperator(
        task_id="upload_categories",
        sql="/sql/ddl/categories.sql",
        conn_id="airflow",
    )
    upload_clients = SQLExecuteQueryOperator(
        task_id="upload_clients",
        sql="/sql/ddl/clients.sql",
        conn_id="airflow",
    )
    upload_products = SQLExecuteQueryOperator(
        task_id="upload_products",
        sql="/sql/ddl/products.sql",
        conn_id="airflow",
    )
    upload_orders = SQLExecuteQueryOperator(
        task_id="upload_orders",
        sql="/sql/ddl/orders.sql",
        conn_id="airflow",
    )
    upload_order_items = SQLExecuteQueryOperator(
        task_id="upload_order_items",
        sql="/sql/ddl/order_items.sql",
        conn_id="airflow",
    )
    cnt_categories = SQLExecuteQueryOperator(
        task_id="cnt_categories",
        sql="/sql/dml/categories.sql",
        conn_id="airflow",
    )
    cnt_clients = SQLExecuteQueryOperator(
        task_id="cnt_clients",
        sql="/sql/dml/clients.sql",
        conn_id="airflow",
    )
    cnt_products = SQLExecuteQueryOperator(
        task_id="cnt_products",
        sql="/sql/dml/products.sql",
        conn_id="airflow",
    )
    cnt_orders = SQLExecuteQueryOperator(
        task_id="cnt_orders",
        sql="/sql/dml/orders.sql",
        conn_id="airflow",
    )
    cnt_order_items = SQLExecuteQueryOperator(
        task_id="cnt_order_items",
        sql="/sql/dml/order_items.sql",
        conn_id="airflow",
    )

    (
        upload_categories, upload_clients 
        >> upload_products, upload_orders 
        >> upload_order_items 
    ) #type: ignore
    upload_categories >> cnt_categories #type: ignore
    upload_clients >> cnt_clients #type: ignore
    upload_products >> cnt_products #type: ignore
    upload_orders >> cnt_orders #type: ignore
    upload_order_items >> cnt_order_items #type: ignore
    check_rows_count(cnt_categories.output)

@task
def check_rows_count(cnt_cat):
    cnt_in_sql = cnt_cat[0][0]
    with open("/data/csv/categories.csv", "r") as file:
        reader = csv.reader(file)
        cnt_in_csv = sum(1 for row in reader) - 1
    if (cnt_in_sql != cnt_in_csv):
        raise Exception(f"Different count of rows in csv ({cnt_in_csv}) and in DB ({cnt_in_sql})")

upload_data__csv()

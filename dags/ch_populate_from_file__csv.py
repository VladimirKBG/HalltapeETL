import pendulum

from airflow.sdk import dag, task
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from clickhouse_driver import Client


def run_clickhouse_sqls(sqls: str, conn_id:str="clickhouse"):
    conn = BaseHook.get_connection(conn_id=conn_id)
    client = Client(
        host=conn.host,
        user=conn.login,
        password=conn.password,
    )
    for sql in sqls:
        client.execute(sql)
    return {"status": "ok"}

@dag(
    start_date=pendulum.datetime(2025, 12, 25, 12, 00),
    dag_id="ch_populate_from_file__csv",
    schedule="0 */2 * * * * *",
    description="Clear tables and upload data into it from csv file.",
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(seconds=15),
        "depends_on_past": False,
    },
    params={
        "schema": "raw",
    },
    tags=["clickhouse", "dml", "csv"],
)
def ch_populate_from_file__csv():
    drop = PythonOperator(
        task_id="clickhouse_populate_tables_from_csv",
        python_callable=run_clickhouse_sqls,
        op_kwargs={
            "sqls": [
                "TRUNCATE TABLE {{ params.schema }}.order_items;",
                "INSERT INTO {{ params.schema }}.order_items SELECT *, now() FROM file('/opt/data_lake/data/csv/order_items.csv', 'CSVWithNames');",
                "TRUNCATE TABLE {{ params.schema }}.orders;",
                "INSERT INTO {{ params.schema }}.orders SELECT *, now() FROM file('/opt/data_lake/data/csv/orders.csv', 'CSVWithNames', 'sk String, bk String, client String, created_at String, closed_at String');",
                "TRUNCATE TABLE {{ params.schema }}.products;",
                "INSERT INTO {{ params.schema }}.products SELECT *, now() FROM file('/opt/data_lake/data/csv/products.csv', 'CSVWithNames', 'sk String, bk String, category Int32, description String, service_time String');",
                "TRUNCATE TABLE {{ params.schema }}.categories;",
                "INSERT INTO {{ params.schema }}.categories SELECT *, now() FROM file('/opt/data_lake/data/csv/categories.csv', 'CSVWithNames');",
                "TRUNCATE TABLE {{ params.schema }}.clients;",
                "INSERT INTO {{ params.schema }}.clients SELECT *, now() FROM file('/opt/data_lake/data/csv/clients.csv', 'CSVWithNames', 'sk String, bk String, inn FixedString(12), ogrn FixedString(15), address String, email String, phone String');",
            ],
            "conn_id": "clickhouse",
        }
    )


ch_populate_from_file__csv()

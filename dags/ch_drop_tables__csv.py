import pendulum

from airflow.sdk import dag, task
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from clickhouse_driver import Client


def run_clickhouse_sql(sqls: str, conn_id:str="clickhouse"):
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
    dag_id="ch_drop_tables__csv",
    schedule=None,
    template_searchpath="/sql_ch",
    description="Drop tables for csv data.",
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(seconds=15),
        "depends_on_past": False,
    },
    params={
        "schema": "raw",
    },
    tags=["clickhouse", "ddl", "csv"],
)
def ch_drop_tables__csv():
    run = PythonOperator(
        task_id="clickhouse_drop_tables_csv",
        python_callable=run_clickhouse_sql,
        op_kwargs={
            "sqls": [
                "DROP TABLE IF EXISTS {{ params.schema }}.order_items;",
                "DROP TABLE IF EXISTS {{ params.schema }}.orders;",
                "DROP TABLE IF EXISTS {{ params.schema }}.products;",
                "DROP TABLE IF EXISTS {{ params.schema }}.clients;",
                "DROP TABLE IF EXISTS {{ params.schema }}.categories;",
            ],
            "conn_id": "clickhouse",
        }
    )


ch_drop_tables__csv()

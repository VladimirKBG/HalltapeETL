import pendulum

from airflow.sdk import dag, task
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from clickhouse_driver import Client

from ch_drop_tables__csv import run_clickhouse_sqls


@dag(
    start_date=pendulum.datetime(2025, 12, 25, 12, 00),
    dag_id="ch_create_tables__csv",
    schedule=None,
    description="Create tables for csv data.",
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
def ch_create_tables__csv():
    create = PythonOperator(
        task_id="clickhouse_create_tables_csv",
        python_callable=run_clickhouse_sqls,
        op_kwargs={
            "sqls": [
                """
                {%- set date_type = 'String' if params.schema == 'raw' else 'DateTime' -%}
                CREATE TABLE {{ params.schema }}.categories 
                    (
                        sk Int32,
                        bk text,
                        category Int32 NULL,
                        description text NULL,
                        uploaded_at DateTime
                        {% if params.schema != 'raw' %}, hash UInt64 {% endif %}
                    ) Engine = MergeTree()
                    PRIMARY KEY (uploaded_at, sk);
                """,
                """
                {%- set date_type = 'String' if params.schema == 'raw' else 'DateTime' -%}
                CREATE TABLE {{ params.schema }}.clients (
                    sk Int32,
                    bk text,
                    inn FixedString(12) NULL,
                    ogrn FixedString(15) NULL,
                    address text NULL,
                    email text NULL,
                    phone text NULL,
                    uploaded_at DateTime
                    {% if params.schema != 'raw' %}, hash UInt64 {% endif %}
                ) Engine = MergeTree()
                PRIMARY KEY (uploaded_at, sk);
                """,
                """
                {%- set date_type = 'String' if params.schema == 'raw' else 'DateTime' -%}
                CREATE TABLE {{ params.schema }}.order_items (
                    sk Int32,
                    bk UUID,
                    order_id Int32,
                    product Int32,
                    amount Int32,
                    price Decimal64(2),
                    discount Decimal64(2) DEFAULT 0,
                    uploaded_at DateTime,
                    {% if params.schema != 'raw' %} hash UInt64, {% endif %}
                    CONSTRAINT order_items_amount_check CHECK (amount > 0),
                    CONSTRAINT order_items_discount_check CHECK (discount >= 0),
                    CONSTRAINT order_items_price_check CHECK (price > 0)
                ) ENGINE = MergeTree()
                PRIMARY KEY (uploaded_at, order_id);
                """,
                """
                {%- set date_type = 'String' if params.schema == 'raw' else 'DateTime' -%}
                CREATE TABLE {{ params.schema }}.products (
                    sk Int32,
                    bk text,
                    category Int32,
                    description text NULL,
                    service_time String,
                    uploaded_at DateTime
                    {% if params.schema != 'raw' %}, hash UInt64 {% endif %}
                ) ENGINE = MergeTree()
                PRIMARY KEY (uploaded_at, sk);
                """,
                """
                {%- set date_type = 'String' if params.schema == 'raw' else 'DateTime' -%}
                CREATE TABLE {{ params.schema }}.orders (
                    sk Int32,
                    bk UUID,
                    client Int32,
                    created_at {{ date_type }},
                    closed_at {{ date_type }} NULL,
                    uploaded_at DateTime
                    {% if params.schema != 'raw' %}, hash UInt64 {% endif %}
                ) ENGINE = MergeTree()
                PRIMARY KEY (created_at, client, sk);
                """,
            ],
            "conn_id": "clickhouse",
        }
    )


ch_create_tables__csv()

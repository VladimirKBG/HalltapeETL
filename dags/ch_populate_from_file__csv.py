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
    start_date=pendulum.datetime(2026, 1, 7, 14, 50),
    dag_id="ch_etl__csv",
    schedule="0/2 * * * *",
    description="Clear tables and upload data into it from csv file.",
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(seconds=15),
        "depends_on_past": False,
    },
    params={
        "raw_schema": "raw",
        "staging_schema": "stg",
        "prod_schema": "prod"
    },
    tags=["clickhouse", "dml", "csv"],
)
def ch_populate_from_file__csv():
    extract = PythonOperator(
        task_id="clickhouse_extract_data_from_csv",
        python_callable=run_clickhouse_sqls,
        op_kwargs={
            "sqls": [
                "TRUNCATE TABLE {{ params.raw_schema }}.order_items;",
                "INSERT INTO {{ params.raw_schema }}.order_items SELECT *, now() FROM file('/opt/data_lake/data/csv/order_items.csv', 'CSVWithNames');",
                "TRUNCATE TABLE {{ params.raw_schema }}.orders;",
                "INSERT INTO {{ params.raw_schema }}.orders SELECT *, now() FROM file('/opt/data_lake/data/csv/orders.csv', 'CSVWithNames', 'sk String, bk String, client String, created_at String, closed_at String');",
                "TRUNCATE TABLE {{ params.raw_schema }}.products;",
                "INSERT INTO {{ params.raw_schema }}.products SELECT *, now() FROM file('/opt/data_lake/data/csv/products.csv', 'CSVWithNames', 'sk String, bk String, category Int32, description String, service_time String');",
                "TRUNCATE TABLE {{ params.raw_schema }}.categories;",
                "INSERT INTO {{ params.raw_schema }}.categories SELECT *, now() FROM file('/opt/data_lake/data/csv/categories.csv', 'CSVWithNames');",
                "TRUNCATE TABLE {{ params.raw_schema }}.clients;",
                "INSERT INTO {{ params.raw_schema }}.clients SELECT *, now() FROM file('/opt/data_lake/data/csv/clients.csv', 'CSVWithNames', 'sk String, bk String, inn FixedString(12), ogrn FixedString(15), address String, email String, phone String');",
            ],
            "conn_id": "clickhouse",
        }
    )
    transform = PythonOperator(
        task_id="clickhouse_transform_data_from_csv",
        python_callable=run_clickhouse_sqls,
        op_kwargs={
            "sqls": [
                """
                INSERT INTO {{ params.staging_schema }}.categories
                SELECT 
                    sk,
                    bk,
                    category,
                    description,
                    uploaded_at,
                    hash
                FROM (
                    SELECT 
                        toInt32(sk) AS sk,
                        bk,
                        toInt32(category) AS category,
                        description,
                        uploaded_at,
                        cityHash64(tuple(
                            toInt32(sk),
                            bk,
                            toInt32(category),
                            description
                        )) AS hash
                    FROM {{ params.raw_schema }}.categories
                ) nv LEFT JOIN (
                    SELECT
                        sk,
                        hash
                    FROM {{ params.staging_schema }}.categories
                ) ov ON nv.sk = ov.sk
                WHERE 
                    nv.hash != ov.hash;
                """,
                """
                INSERT INTO {{ params.staging_schema }}.clients
                SELECT 
                    sk,
                    bk,
                    inn,
                    ogrn,
                    address,
                    email,
                    phone,
                    uploaded_at,
                    hash
                FROM (
                    SELECT 
                        toInt32(sk) AS sk,
                        bk,
                        inn,
                        ogrn,
                        address,
                        email,
                        phone,
                        uploaded_at,
                        cityHash64(tuple(
                            toInt32(sk),
                            bk,
                            inn,
                            ogrn,
                            address,
                            email,
                            phone
                        )) AS hash
                    FROM {{ params.raw_schema }}.clients
                ) nv LEFT JOIN (
                    SELECT
                        sk,
                        hash
                    FROM {{ params.staging_schema }}.clients
                ) ov ON nv.sk = ov.sk
                WHERE 
                    nv.hash != ov.hash;
                """,
                """
                INSERT INTO {{ params.staging_schema }}.orders
                SELECT 
                    sk,
                    bk,
                    client,
                    created_at,
                    closed_at,
                    uploaded_at,
                    hash
                FROM (
                    SELECT 
                        toInt32(sk) AS sk,
                        bk,
                        client,
                        parseDateTimeBestEffort(created_at) AS created_at,
                        parseDateTimeBestEffortOrNull(closed_at) AS closed_at,
                        uploaded_at,
                        cityHash64(tuple(
                            toInt32(sk),
                            bk,
                            client,
                            created_at,
                            closed_at
                        )) AS hash
                    FROM {{ params.raw_schema }}.orders
                ) nv LEFT JOIN (
                    SELECT
                        sk,
                        hash
                    FROM stg.orders
                ) ov ON nv.sk = ov.sk
                WHERE 
                    nv.hash != ov.hash;
                """,
                """
                INSERT INTO {{ params.staging_schema }}.products
                SELECT 
                    sk,
                    bk,
                    category,
                    description,
                    service_time,
                    uploaded_at,
                    hash
                FROM (
                    SELECT 
                        toInt32(sk) AS sk,
                        bk,
                        toInt32(category) AS category,
                        description,
                        service_time,
                        uploaded_at,
                        cityHash64(tuple(
                            toInt32(sk),
                            bk,
                            category,
                            description,
                            service_time
                        )) AS hash
                    FROM {{ params.raw_schema }}.products
                ) nv LEFT JOIN (
                    SELECT
                        sk,
                        hash
                    FROM {{ params.staging_schema }}.products
                ) ov ON nv.sk = ov.sk
                WHERE 
                    nv.hash != ov.hash;
                """,
                """
                DROP TABLE IF EXISTS {{ params.staging_schema }}.order_items_new_values;
                """,
                """
                CREATE TABLE {{ params.staging_schema }}.order_items_new_values (
                    sk Int32,
                    bk UUID,
                    order_id Int32,
                    product Int32,
                    amount Int32,
                    price Decimal64(2),
                    discount Decimal64(2),
                    uploaded_at timestamp,
                    hash UInt64,
                    CONSTRAINT order_items_amount_check CHECK (amount > 0),
                    CONSTRAINT order_items_discount_check CHECK (discount >= 0),
                    CONSTRAINT order_items_price_check CHECK (price > 0)
                ) ENGINE = Memory;
                """,
                """
                INSERT INTO {{ params.staging_schema }}.order_items_new_values
                SELECT 
                    sk,
                    bk,
                    order_id,
                    product,
                    amount,
                    price,
                    discount,
                    uploaded_at,
                    hash
                FROM (
                    SELECT 
                        toInt32(sk) AS sk,
                        bk,
                        toInt32(order_id) AS order_id,
                        toInt32(product) AS product,
                        toInt32(amount) AS amount,
                        toDecimal64(price, 2) AS price,
                        toDecimal64(discount, 2) AS discount,
                        uploaded_at,
                        cityHash64(tuple(
                            sk,
                            bk,
                            order_id,
                            product,
                            amount,
                            price,
                            discount
                        )) AS hash
                    FROM {{ params.raw_schema }}.order_items
                ) nv LEFT JOIN (
                    SELECT
                        sk,
                        hash
                    FROM {{ params.staging_schema }}.order_items
                ) ov ON nv.sk = ov.sk
                WHERE 
                    nv.hash != ov.hash;	
                """,
                """
                INSERT INTO {{ params.staging_schema }}.order_items
                SELECT * FROM {{ params.staging_schema }}.order_items_new_values;
                """,
            ],
            "conn_id": "clickhouse",
        }
    )
    extract >> transform #type: ignore

ch_populate_from_file__csv()

CREATE TABLE raw.categories 
(
	sk Int32,
	bk text,
	category Int32 NULL,
	description text NULL,
	uploaded_at timestamp
) Engine = MergeTree()
PRIMARY KEY (uploaded_at, sk);
ALTER TABLE raw.categories MODIFY COLUMN uploaded_at timestamp;
DROP TABLE raw.categories;

CREATE TABLE raw.clients (
	sk Int32 NOT NULL,
	bk text NOT NULL,
	inn varchar(12) NULL,
	ogrn varchar(15) NULL,
	address text NULL,
	email text NULL,
	phone text NULL,
	uploaded_at timestamp NOT NULL
)
Engine = MergeTree()
PRIMARY KEY (uploaded_at, sk);

CREATE TABLE raw.order_items (
	sk Int32 NOT NULL,
	bk UUID NOT NULL,
	order_id Int32 NOT NULL,
	product Int32 NOT NULL,
	amount Int32 NOT NULL,
	price numeric(12, 2) NOT NULL,
	discount numeric(2, 2) DEFAULT 0 NOT NULL,
	uploaded_at timestamp NOT NULL,
	CONSTRAINT order_items_amount_check CHECK (amount > 0),
	CONSTRAINT order_items_discount_check CHECK (discount >= 0),
	CONSTRAINT order_items_price_check CHECK (price > 0)
) ENGINE = MergeTree()
PRIMARY KEY (uploaded_at, order_id);


CREATE TABLE raw.products (
	sk Int32 NOT NULL,
	bk text NOT NULL,
	category Int32 NOT NULL,
	description text NULL,
	service_time UInt8 NOT NULL,
	uploaded_at timestamp NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (uploaded_at, sk);

CREATE TABLE raw.orders (
	sk Int32 NOT NULL,
	bk UUID NOT NULL,
	client Int32 NOT NULL,
	created_at timestamp NOT NULL,
	closed_at timestamp NULL,
	uploaded_at timestamp NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (created_at, client, sk);

INSERT INTO raw.order_items SELECT *, now() FROM file('/opt/data_lake/data/csv/order_items.csv', 'CSVWithNames');
TRUNCATE TABLE raw.order_items;

DROP TABLE raw.order_items;
SELECT DISTINCT uploaded_at FROM raw.order_items FINAL;

CREATE TABLE raw.order_items (
	sk Int32 NOT NULL,
	bk UUID NOT NULL,
	order_id Int32 NOT NULL,
	product Int32 NOT NULL,
	amount Int32 NOT NULL,
	price numeric(12, 2) NOT NULL,
	discount numeric(2, 2) DEFAULT 0 NOT NULL,
	uploaded_at timestamp NOT NULL,
	version Int32,
	CONSTRAINT order_items_amount_check CHECK (amount > 0),
	CONSTRAINT order_items_discount_check CHECK (discount >= 0),
	CONSTRAINT order_items_price_check CHECK (price > 0)
) ENGINE = ReplacingMergeTree(version)
PRIMARY KEY (uploaded_at, order_id);

CREATE TABLE stg.order_items_new_values (
	sk Int32,
	bk UUID,
	order_id Int32,
	product Int32,
	amount Int32,
	price Decimal64(2),
	discount Decimal32(2) DEFAULT 0,
	uploaded_at timestamp,
	version Int32,
	CONSTRAINT order_items_amount_check CHECK (amount > 0),
	CONSTRAINT order_items_discount_check CHECK (discount >= 0),
	CONSTRAINT order_items_price_check CHECK (price > 0)
) ENGINE = Memory;

DROP TABLE stg.order_items_new_values ;

CREATE TABLE prod.ordered_products_count (
	id String,
	ordered_amount Int32,
	earned_total Decimal64(2),
	discount_total Decimal64(2)
) ENGINE = SummingMergeTree
PRIMARY KEY (id);

DROP TABLE prod.ordered_products_count;

CREATE MATERIALIZED VIEW prod.ordered_products_count_mv TO prod.ordered_products_count AS
SELECT 
	p.bk AS id,
	sum(oi.amount) AS ordered_amount,
	sum(oi.price*oi.amount) AS earned_total,
	sum(oi.price*oi.amount*oi.discount) AS discount_total
FROM stg.order_items oi
LEFT JOIN stg.products p ON oi.product = p.sk
GROUP BY (p.bk);

SELECT *
FROM prod.ordered_products_count;


SELECT database, name, create_table_query
FROM system.tables
WHERE database = 'prod' AND name = 'ordered_products_count_mv';
SELECT count() FROM prod.ordered_products_count;
SELECT name, active, min_date, max_date FROM system.parts
WHERE database='prod' AND table='ordered_products_count' LIMIT 50;




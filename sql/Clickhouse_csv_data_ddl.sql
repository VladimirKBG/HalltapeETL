CREATE TABLE raw.categories 
(
	sk Int32 NOT NULL,
	bk text NOT NULL,
	category Int32 NULL,
	description text NULL,
	uploaded_at timestamp NOT NULL
) Engine = MergeTree()
PRIMARY KEY (uploaded_at, sk);;
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




















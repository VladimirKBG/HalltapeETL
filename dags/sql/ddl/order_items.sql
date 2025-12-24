DROP TABLE IF EXISTS raw.dim_order_items_temp CASCADE;

CREATE TABLE raw.dim_order_items_temp (
	sk int4 NOT NULL,
	bk uuid NOT NULL,
	order_id int4 NOT NULL,
	product int4 NOT NULL,
	amount int4 NOT NULL,
	price numeric(12, 2) NOT NULL,
	discount numeric(2, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dim_order_items_amount_check_temp CHECK ((amount > 0)),
	CONSTRAINT dim_order_items_discount_check_temp CHECK ((discount >= (0)::numeric)),
	CONSTRAINT dim_order_items_pkey_temp PRIMARY KEY (sk),
	CONSTRAINT dim_order_items_price_check_temp CHECK ((price > (0)::numeric)),
	CONSTRAINT dim_order_items_order_id_fkey_temp FOREIGN KEY (order_id) REFERENCES raw.fct_orders_temp(sk),
	CONSTRAINT dim_order_items_product_fkey_temp FOREIGN KEY (product) REFERENCES raw.dim_products_temp(sk)
);

COPY raw.dim_order_items_temp FROM '/csv/order_items.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

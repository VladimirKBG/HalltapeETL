DROP TABLE IF EXISTS raw.fct_orders_temp CASCADE;

CREATE TABLE raw.fct_orders_temp (
	sk int4 NOT NULL,
	bk uuid NOT NULL,
	client int4 NOT NULL,
	created_at timestamp NOT NULL,
	closed_at timestamp NULL,
	CONSTRAINT fct_order_bk_key_temp UNIQUE (bk),
	CONSTRAINT fct_order_pkey_temp PRIMARY KEY (sk),
	CONSTRAINT fct_order_client_fkey_temp FOREIGN KEY (client) REFERENCES raw.dim_clients_temp(sk)
);

COPY raw.fct_orders_temp FROM '/csv/orders.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

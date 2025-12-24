DROP TABLE IF EXISTS raw.dim_products_temp CASCADE;

CREATE TABLE raw.dim_products_temp (
	sk int4 NOT NULL,
	bk text NOT NULL,
	category int4 NOT NULL,
	description text NULL,
	service_time interval NOT NULL,
	CONSTRAINT dim_products_pkey_temp PRIMARY KEY (sk),
	CONSTRAINT dim_products_category_fkey_temp FOREIGN KEY (category) REFERENCES raw.dim_categories_temp(sk)
);

COPY raw.dim_products_temp FROM '/csv/products.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

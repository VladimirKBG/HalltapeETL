DROP TABLE IF EXISTS raw.dim_categories_temp CASCADE;

CREATE TABLE raw.dim_categories_temp (
	sk int4 NOT NULL,
	bk text NOT NULL,
	category int4 NULL,
	description text NULL,
	CONSTRAINT dim_category_pkey_temp PRIMARY KEY (sk),
	CONSTRAINT dim_category_category_fkey_temp FOREIGN KEY (category) REFERENCES raw.dim_categories_temp(sk)
);

COPY raw.dim_categories_temp FROM '/csv/categories.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');
DROP TABLE IF EXISTS raw.dim_clients_temp CASCADE;

CREATE TABLE raw.dim_clients_temp (
	sk int4 NOT NULL,
	bk text NOT NULL,
	inn varchar(12) NULL,
	ogrn varchar(15) NULL,
	address text NULL,
	email text NULL,
	phone text NULL,
	CONSTRAINT dim_clients_inn_key_temp UNIQUE (inn),
	CONSTRAINT dim_clients_ogrn_key_temp UNIQUE (ogrn),
	CONSTRAINT dim_clients_pkey_temp PRIMARY KEY (sk)
);

COPY raw.dim_clients_temp FROM '/csv/clients.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',');

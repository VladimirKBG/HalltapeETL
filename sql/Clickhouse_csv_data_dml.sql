INSERT INTO stg.categories
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
	FROM raw.categories
) nv LEFT JOIN (
	SELECT
		sk,
		hash
	FROM stg.categories
) ov ON nv.sk = ov.sk
WHERE 
	nv.hash != ov.hash;

SELECT parseDateTimeBestEffort('1978-04-08T20:12:46.705813+00:00');
SELECT cityHash64(tuple(('1978-04-08 20:12:46')));

TRUNCATE TABLE stg.categories;

INSERT INTO stg.clients
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
	FROM raw.clients
) nv LEFT JOIN (
	SELECT
		sk,
		hash
	FROM stg.clients
) ov ON nv.sk = ov.sk
WHERE 
	nv.hash != ov.hash;

INSERT INTO stg.orders
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
	FROM raw.orders
) nv LEFT JOIN (
	SELECT
		sk,
		hash
	FROM stg.orders
) ov ON nv.sk = ov.sk
WHERE 
	nv.hash != ov.hash;

INSERT INTO stg.products
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
	FROM raw.products
) nv LEFT JOIN (
	SELECT
		sk,
		hash
	FROM stg.products
) ov ON nv.sk = ov.sk
WHERE 
	nv.hash != ov.hash;



INSERT INTO raw.order_items_new_values
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
	FROM raw.order_items
) nv LEFT JOIN (
	SELECT
		sk,
		hash
	FROM stg.order_items
) ov ON nv.sk = ov.sk
WHERE 
	nv.hash != ov.hash;	
	
TRUNCATE TABLE stg.order_items;
SELECT *
FROM stg.order_items
WHERE sk IN (	
	SELECT sk
	FROM stg.order_items
	GROUP BY sk
	HAVING count(*)>1
) ORDER BY sk;
	
	
SELECT count(*)
FROM raw.order_items oi ;

SELECT count(*)
FROM prod.ordered_products_count;

SELECT count(), min(_part_index) FROM stg.order_items;

INSERT INTO stg.order_items
(sk, bk, order_id, product, amount, price, discount, uploaded_at, hash)
VALUES (0, 'cd613e30-d8f1-4adf-91b7-584a2265b1f5', 0, 1, 1, 1, 0, now(), cityHash64(0));

SELECT count() FROM stg.order_items
WHERE uploaded_at >= now() - INTERVAL 10 MINUTE;

SELECT * FROM stg.order_items_new_values oinv ;

SELECT event_time, query, exception, stack_trace
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND exception != ''
ORDER BY event_time DESC
LIMIT 50;




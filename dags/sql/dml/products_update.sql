INSERT INTO raw.dim_products AS t
SELECT * FROM raw.dim_products_temp AS s
ON CONFLICT (sk)
DO UPDATE
    SET bk = EXCLUDED.bk,
        category = EXCLUDED.category,
        description = EXCLUDED.description,
        service_time = EXCLUDED.service_time
    WHERE t.bk IS DISTINCT FROM EXCLUDED.bk OR
        t.category IS DISTINCT FROM EXCLUDED.category OR
        t.description IS DISTINCT FROM EXCLUDED.description OR
		t.service_time IS DISTINCT FROM EXCLUDED.service_time;
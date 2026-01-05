INSERT INTO raw.dim_categories AS t
SELECT * FROM raw.dim_categories_temp AS s
ON CONFLICT (sk)
DO UPDATE
    SET bk = EXCLUDED.bk,
        category = EXCLUDED.category,
        description = EXCLUDED.description
    WHERE t.bk IS DISTINCT FROM EXCLUDED.bk OR
        t.category IS DISTINCT FROM EXCLUDED.category OR
        t.description IS DISTINCT FROM EXCLUDED.description;
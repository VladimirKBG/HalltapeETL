INSERT INTO raw.fct_orders AS t
SELECT * FROM raw.fct_orders_temp AS s
ON CONFLICT (sk)
DO UPDATE
    SET bk = EXCLUDED.bk,
        client = EXCLUDED.client,
        created_at = EXCLUDED.created_at,
        closed_at = EXCLUDED.closed_at
    WHERE t.bk IS DISTINCT FROM EXCLUDED.bk OR
        t.client IS DISTINCT FROM EXCLUDED.client OR
        t.created_at IS DISTINCT FROM EXCLUDED.created_at OR
        t.closed_at IS DISTINCT FROM EXCLUDED.closed_at;
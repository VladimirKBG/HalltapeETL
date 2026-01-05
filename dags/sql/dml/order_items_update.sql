INSERT INTO raw.dim_order_items AS t
SELECT * FROM raw.dim_order_items_temp AS s
ON CONFLICT (sk)
DO UPDATE
    SET bk = EXCLUDED.bk,
        order_id = EXCLUDED.order_id,
        product = EXCLUDED.product,
        amount = EXCLUDED.amount,
        price = EXCLUDED.price,
        discount = EXCLUDED.discount
    WHERE t.bk IS DISTINCT FROM EXCLUDED.bk OR
        t.order_id IS DISTINCT FROM EXCLUDED.order_id OR
        t.product IS DISTINCT FROM EXCLUDED.product OR
        t.amount IS DISTINCT FROM EXCLUDED.amount OR
        t.price IS DISTINCT FROM EXCLUDED.price OR
        t.discount IS DISTINCT FROM EXCLUDED.discount;
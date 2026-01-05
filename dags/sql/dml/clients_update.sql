INSERT INTO raw.dim_clients AS t
SELECT * FROM raw.dim_clients_temp AS s
ON CONFLICT (sk)
DO UPDATE
    SET bk = EXCLUDED.bk,
        inn = EXCLUDED.inn,
        ogrn = EXCLUDED.ogrn,
        address = EXCLUDED.address,
        email = EXCLUDED.email,
        phone = EXCLUDED.phone
    WHERE t.bk IS DISTINCT FROM EXCLUDED.bk OR
        t.inn IS DISTINCT FROM EXCLUDED.inn OR
        t.ogrn IS DISTINCT FROM EXCLUDED.ogrn OR
        t.address IS DISTINCT FROM EXCLUDED.address OR
        t.email IS DISTINCT FROM EXCLUDED.email OR
        t.phone IS DISTINCT FROM EXCLUDED.phone;
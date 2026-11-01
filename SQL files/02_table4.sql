INSERT INTO public.table4 (
    id_column_1,
    text_column_1,
    text_column_2
)
SELECT
    (
        (ASCII(LEFT(dedup.tc3, 1)) - 65) * 26
        + (ASCII(RIGHT(dedup.tc3, 1)) - 65)
        + 1
    )::smallint,
    'SYNTHETIC/GENERIC',
    dedup.tc3
FROM (
    SELECT DISTINCT
        UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_3')) AS tc3
    FROM {{source}} AS source_row
) AS dedup
WHERE COALESCE(dedup.tc3, '') ~ '^[A-Z]{2}$';

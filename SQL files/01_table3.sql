INSERT INTO public.table3 (
    text_column_1,
    text_column_2,
    numeric_column_1,
    text_column_3,
    text_column_4
)
SELECT DISTINCT
    dedup.tc1,
    'SYNTHETIC/REFERENCE',
    NULL::numeric,
    LEFT(dedup.tc2, 1),
    RIGHT(dedup.tc2, 1)
FROM (
    SELECT DISTINCT
        BTRIM(v.j ->> 'text_column_1') AS tc1,
        UPPER(BTRIM(v.j ->> 'text_column_2')) AS tc2
    FROM {{source}} AS source_row
    CROSS JOIN LATERAL (
        SELECT source_row.raw -> 'values' AS j
    ) AS v
) AS dedup
WHERE COALESCE(dedup.tc1, '') <> ''
  AND COALESCE(dedup.tc2, '') ~ '^[A-Z]{2}$';

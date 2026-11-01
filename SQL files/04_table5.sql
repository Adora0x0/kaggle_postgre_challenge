INSERT INTO public.table5 (
    numeric_column_1,
    numeric_column_2,
    integer_column_1,
    integer_column_2,
    id_column_2,
    id_column_3
)
SELECT
    ROUND(parsed.ic1, 1),
    NULL::numeric,
    NULL::integer,
    NULL::integer,
    main_row.id_column_1,
    NULL::bigint
FROM {{source}} AS source_row
CROSS JOIN LATERAL (
    SELECT NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_1'), '')::numeric AS ic1
) AS parsed
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE parsed.ic1 > 0;

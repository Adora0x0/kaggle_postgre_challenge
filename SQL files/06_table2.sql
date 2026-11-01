INSERT INTO public.table2 (
    id_column_2,
    integer_column_1,
    integer_column_2,
    text_column_1,
    integer_column_3,
    integer_column_4,
    integer_column_5,
    integer_column_6
)
SELECT
    main_row.id_column_1,
    position_data.position_number,
    ROUND(extracted.weight_num * 1000)::integer,
    extracted.type_clean,
    NULL::integer,
    NULL::integer,
    NULL::integer,
    CASE
        WHEN extracted.gap_num > 0
        THEN ROUND(extracted.gap_num * 1000)::integer
        ELSE NULL::integer
    END
FROM {{source}} AS source_row
CROSS JOIN LATERAL (
    SELECT source_row.raw -> 'values' AS j
) AS v
CROSS JOIN LATERAL (
    VALUES
        (1::smallint, v.j ->> 'integer_column_2', 'text_column_4', 'numeric_column_3', 'numeric_column_4'),
        (2::smallint, v.j ->> 'integer_column_3', 'text_column_5', 'numeric_column_5', 'numeric_column_6'),
        (3::smallint, v.j ->> 'integer_column_4', 'text_column_6', 'numeric_column_7', 'numeric_column_8'),
        (4::smallint, v.j ->> 'integer_column_5', 'text_column_7', 'numeric_column_9', 'numeric_column_10'),
        (5::smallint, v.j ->> 'integer_column_6', 'text_column_8', 'numeric_column_11', 'numeric_column_12'),
        (6::smallint, v.j ->> 'integer_column_7', 'text_column_9', 'numeric_column_13', 'numeric_column_14'),
        (7::smallint, v.j ->> 'integer_column_8', 'text_column_10', 'numeric_column_15', 'numeric_column_16'),
        (8::smallint, v.j ->> 'integer_column_9', 'text_column_11', 'numeric_column_17', 'numeric_column_18'),
        (9::smallint, v.j ->> 'integer_column_10', 'text_column_12', 'numeric_column_19', 'numeric_column_20'),
        (10::smallint, v.j ->> 'integer_column_11', 'text_column_13', 'numeric_column_21', 'numeric_column_22'),
        (11::smallint, v.j ->> 'integer_column_12', 'text_column_14', 'numeric_column_23', 'numeric_column_24'),
        (12::smallint, v.j ->> 'integer_column_13', 'text_column_15', 'numeric_column_25', 'numeric_column_26'),
        (13::smallint, v.j ->> 'integer_column_14', 'text_column_16', 'numeric_column_27', 'numeric_column_28'),
        (14::smallint, v.j ->> 'integer_column_15', 'text_column_17', 'numeric_column_29', 'numeric_column_30')
) AS position_data(position_number, declared_number_text, type_key, weight_key, gap_key)
CROSS JOIN LATERAL (
    SELECT
        UPPER(BTRIM(v.j ->> position_data.type_key)) AS type_clean,
        NULLIF(BTRIM(v.j ->> position_data.weight_key), '')::numeric AS weight_num,
        NULLIF(BTRIM(v.j ->> position_data.gap_key), '')::numeric AS gap_num
    WHERE position_data.declared_number_text <> ''
      AND NULLIF(BTRIM(position_data.declared_number_text), '')::smallint
            = position_data.position_number
) AS extracted
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE extracted.weight_num >= 0
  AND extracted.type_clean IN ('X', 'Y', 'Z');

SELECT
    COALESCE(gender, 'UNK')                     AS gender,
    COALESCE(source_year, -1)                   AS source_year,
    COALESCE(active_ingredient_name, 'UNKNOWN') AS active_ingredient_name,
    COALESCE(brand_name, 'UNKNOWN')             AS brand_name,
    COUNT(*)                                    AS report_count
FROM {{ ref('dim_drug_faers') }}
GROUP BY gender, source_year, active_ingredient_name, brand_name
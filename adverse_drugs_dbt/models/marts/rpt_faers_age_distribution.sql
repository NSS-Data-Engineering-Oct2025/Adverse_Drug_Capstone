SELECT
    CASE
        WHEN age_in_years < 5   THEN 'Under 5'
        WHEN age_in_years < 10  THEN '5-9'
        WHEN age_in_years < 15  THEN '10-14'
        WHEN age_in_years < 20  THEN '15-19'
        WHEN age_in_years < 25  THEN '20-24'
        WHEN age_in_years < 35  THEN '25-34'
        WHEN age_in_years < 45  THEN '35-44'
        WHEN age_in_years < 55  THEN '45-54'
        WHEN age_in_years < 60  THEN '55-59'
        WHEN age_in_years < 65  THEN '60-64'
        WHEN age_in_years < 75  THEN '65-74'
        WHEN age_in_years < 85  THEN '75-84'
        WHEN age_in_years >= 85 THEN '85+'
    END                  AS age_group,
    COUNT(*)                        AS report_count,
    COALESCE(source_year, -1)       AS source_year,
    COALESCE(gender, 'UNK')         AS gender,
    COALESCE(active_ingredient_name, 'UNKNOWN') AS active_ingredient_name,
    COALESCE(brand_name, 'UNKNOWN')             AS brand_name
FROM {{ ref('dim_drug_faers') }}
WHERE age_in_years IS NOT NULL
GROUP BY age_group, source_year, gender, active_ingredient_name, brand_name
ORDER BY MIN(age_in_years)
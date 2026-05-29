SELECT
    -- Grouping dimensions for all dashboard filters
    COALESCE(overall_status, 'UNKNOWN')      AS overall_status,
    COALESCE(phase_bucket, 'Unknown')        AS phase_bucket,
    COALESCE(lead_sponsor_class, 'OTHER')    AS sponsor_class,
    COALESCE(eligible_sex, 'ALL')            AS eligible_sex,
    COALESCE(study_type, 'UNKNOWN')          AS study_type,
    COALESCE(first_location_country, 'UNKNOWN') AS country,
    is_industry_sponsored,
    has_drug_intervention,

    -- Eligibility age range bucket (mirrors FAERS age groups for comparison)
    CASE
        WHEN minimum_age_years IS NULL                          THEN 'Unknown'
        WHEN minimum_age_years < 18                            THEN 'Includes children (<18)'
        WHEN minimum_age_years >= 18 AND minimum_age_years < 65 THEN 'Adults (18-64)'
        WHEN minimum_age_years >= 65                           THEN 'Older adults (65+)'
    END                                      AS eligibility_age_bucket,

    -- Aggregated measures
    COUNT(*)                                 AS study_count,
    SUM(enrollment)                          AS total_enrollment,
    AVG(enrollment)                          AS avg_enrollment,
    MEDIAN(enrollment)                       AS median_enrollment,
    AVG(study_duration_days)                 AS avg_duration_days,
    MEDIAN(study_duration_days)              AS median_duration_days,
    SUM(location_count)                      AS total_locations,
    COUNT(CASE WHEN has_drug_intervention THEN 1 END) AS drug_study_count

FROM {{ ref('dim_clinical_trials') }}
GROUP BY
    overall_status,
    phase_bucket,
    sponsor_class,
    eligible_sex,
    study_type,
    country,
    is_industry_sponsored,
    has_drug_intervention,
    eligibility_age_bucket
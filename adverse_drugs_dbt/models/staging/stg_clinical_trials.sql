SELECT
    -- ── Identification ─────────────────────────────────────────────────────
    "nct_id"::STRING                                        AS nct_id,
    "brief_title"::STRING                                   AS brief_title,
    "official_title"::STRING                                AS official_title,
    "org_study_id"::STRING                                  AS org_study_id,

    -- ── Status ─────────────────────────────────────────────────────────────
    "overall_status"::STRING                                AS overall_status,
    TRY_TO_DATE("start_date"::STRING)                       AS start_date,
    TRY_TO_DATE("primary_completion_date"::STRING)          AS primary_completion_date,
    TRY_TO_DATE("completion_date"::STRING)                  AS completion_date,
    TRY_TO_DATE("study_first_posted"::STRING)               AS study_first_posted,
    TRY_TO_DATE("last_update_posted"::STRING)               AS last_update_posted,

    -- ── Design ─────────────────────────────────────────────────────────────
    "study_type"::STRING                                    AS study_type,
    "phases"::STRING                                        AS phases,
    TRY_TO_NUMBER("enrollment"::STRING)                     AS enrollment,
    "enrollment_type"::STRING                               AS enrollment_type,
    "allocation"::STRING                                    AS allocation,
    "intervention_model"::STRING                            AS intervention_model,
    "primary_purpose"::STRING                               AS primary_purpose,
    "masking"::STRING                                       AS masking,

    -- ── Eligibility ────────────────────────────────────────────────────────
    "sex"::STRING                                           AS eligible_sex,
    "healthy_volunteers"::STRING                            AS healthy_volunteers,
    -- Normalize age strings to numeric years (e.g. "18 Years" → 18)
    TRY_TO_NUMBER(
        REGEXP_SUBSTR("minimum_age"::STRING, '[0-9]+')
    )                                                       AS minimum_age_years,
    TRY_TO_NUMBER(
        REGEXP_SUBSTR("maximum_age"::STRING, '[0-9]+')
    )                                                       AS maximum_age_years,
    "std_ages"::STRING                                      AS std_ages,   -- e.g. CHILD|ADULT|OLDER_ADULT

    -- ── Sponsor ────────────────────────────────────────────────────────────
    "lead_sponsor_name"::STRING                             AS lead_sponsor_name,
    "lead_sponsor_class"::STRING                            AS lead_sponsor_class,   -- INDUSTRY | NIH | OTHER_GOV | OTHER

    -- ── Conditions & Interventions ─────────────────────────────────────────
    "conditions"::STRING                                    AS conditions,           -- pipe-delimited
    "intervention_names"::STRING                            AS intervention_names,   -- pipe-delimited
    "intervention_types"::STRING                            AS intervention_types,   -- pipe-delimited
    "drug_interventions"::STRING                            AS drug_interventions,   -- pipe-delimited, DRUG type only

    -- ── Outcomes & Geography ───────────────────────────────────────────────
    "primary_outcome"::STRING                               AS primary_outcome,
    TRY_TO_NUMBER("location_count"::STRING)                 AS location_count,
    "first_location_country"::STRING                        AS first_location_country,

    -- ── Derived convenience columns ────────────────────────────────────────
    -- Study duration in days (NULL if either date is missing)
    DATEDIFF(
        'day',
        TRY_TO_DATE("start_date"::STRING),
        TRY_TO_DATE("completion_date"::STRING)
    )                                                       AS study_duration_days,

    -- Broad phase bucket for grouping
    CASE
        WHEN "phases"::STRING ILIKE '%PHASE1%' AND "phases"::STRING ILIKE '%PHASE2%' THEN 'Phase 1/2'
        WHEN "phases"::STRING ILIKE '%PHASE2%' AND "phases"::STRING ILIKE '%PHASE3%' THEN 'Phase 2/3'
        WHEN "phases"::STRING ILIKE '%PHASE1%' THEN 'Phase 1'
        WHEN "phases"::STRING ILIKE '%PHASE2%' THEN 'Phase 2'
        WHEN "phases"::STRING ILIKE '%PHASE3%' THEN 'Phase 3'
        WHEN "phases"::STRING ILIKE '%PHASE4%' THEN 'Phase 4'
        WHEN "phases"::STRING ILIKE '%NA%'     THEN 'N/A'
        ELSE 'Unknown'
    END                                                     AS phase_bucket,

    -- Has drug intervention flag
    CASE
        WHEN "drug_interventions"::STRING IS NOT NULL
            AND LENGTH(TRIM("drug_interventions"::STRING)) > 0
        THEN TRUE
        ELSE FALSE
    END                                                     AS has_drug_intervention,

    -- Is industry sponsored
    CASE
        WHEN "lead_sponsor_class"::STRING = 'INDUSTRY' THEN TRUE
        ELSE FALSE
    END                                                     AS is_industry_sponsored

FROM {{ source('ADVERSE_DRUGS', 'CLINICAL_TRIALS') }}
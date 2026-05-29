SELECT
    -- ── Identifiers ────────────────────────────────────────────────────────────
    nct_id,
    brief_title,
    official_title,
    org_study_id,

    -- ── Status & Dates ─────────────────────────────────────────────────────────
    overall_status,
    start_date,
    primary_completion_date,
    completion_date,
    study_first_posted,
    last_update_posted,
    study_duration_days,

    -- ── Study Design ───────────────────────────────────────────────────────────
    study_type,
    phases,
    phase_bucket,
    enrollment,
    enrollment_type,
    allocation,
    intervention_model,
    primary_purpose,
    masking,

    -- ── Eligibility / Demographics ─────────────────────────────────────────────
    eligible_sex,
    healthy_volunteers,
    minimum_age_years,
    maximum_age_years,
    std_ages,

    -- ── Sponsor ────────────────────────────────────────────────────────────────
    lead_sponsor_name,
    lead_sponsor_class,
    is_industry_sponsored,

    -- ── Conditions & Interventions ─────────────────────────────────────────────
    conditions,
    intervention_names,
    intervention_types,
    drug_interventions,
    has_drug_intervention,

    -- ── Outcomes & Geography ───────────────────────────────────────────────────
    primary_outcome,
    location_count,
    first_location_country

FROM {{ ref('stg_clinical_trials') }}
with 
demo_faers_init as (
    select 
    "primaryid"::BIGINT as primary_id,
    "i_f_code"::string as if_code,
    "event_dt"::date as event_date,
    "mfr_dt"::date as manufacturer_received_date,
    "init_fda_dt"::date as initial_fda_received_date,
    "fda_dt"::date as current_fda_received_date,
    "rept_cod"::string as report_code,
    "mfr_num"::string as manufacturer_number,
    "mfr_sndr"::string as manufacturer_name,
    CASE 
        WHEN "age_cod" = 'DY' THEN ("age"::float / 365.25)::int -- Convert days to years
        WHEN "age_cod" = 'MON' THEN ("age"::float / 12)::int -- Convert months to years
        WHEN "age_cod" = 'YR' THEN ("age"::float)::int -- Already in years
        WHEN "age_cod" = 'DEC' THEN ("age"::float * 10)::int -- Convert decades to years
        WHEN "age_cod" = 'WK' THEN ("age"::float / 52.1775)::int -- Convert weeks to years
        ELSE NULL -- For unknown or other age codes
    END as age_in_years,
    CASE
        WHEN "sex" IS NULL THEN 
        CASE
            WHEN "gndr_cod" = 'M' THEN 'M'
            WHEN "gndr_cod" = 'F' THEN 'F'
            ELSE NULL -- For unknown or other gndr_cod values
        END
        WHEN "sex" = 'M' THEN 'M' -- Use sex as the primary gender field
        WHEN "sex" = 'F' THEN 'F'
        ELSE NULL
    END as gender,
    CASE 
        WHEN "wt_cod" = 'KG' THEN ROUND("wt"::float*2.20462, 1) -- Convert kilograms to pounds
        WHEN "wt_cod" = 'G' THEN ROUND("wt"::float / 1000 * 2.20462, 1) -- Convert grams to pounds
        WHEN "wt_cod" = 'LBS' THEN ROUND("wt"::float, 1) -- Already in pounds
        ELSE NULL -- For unknown or other weight codes
    END as weight_in_lbs

    from {{ source('ADVERSE_DRUGS', 'DEMO_FAERS') }}
),

drug_faers_init as (
    select 
    CONCAT("caseid"::string, "drug_seq"::string)::BIGINT as primary_id,
    "role_cod"::string as role_code,
    "drugname"::string as drug_name,
    CASE
        WHEN "val_vbm"::int = 1 THEN TRUE -- Convert val_vbm to boolean (1 = true, 0 = false)
        WHEN "val_vbm"::int = 2 THEN FALSE
        ELSE NULL -- For unknown or other values
    END as is_validated_trade_name,
    "route"::string as route,
    "dose_vbm"::string as dose,
    "cum_dose_chr"::string as cumulative_dose,
    "dechal"::string as dechallenge,
    "rechal"::string as rechallenge,
    CASE
        WHEN "lot_num" IS NULL THEN "lot_nbr"::string -- Use lot_nbr if lot_num is null
        ELSE "lot_nbr"::string
    END as lot_number,
    "exp_dt"::date as expiration_date,
    "nda_num"::BIGINT as nda_number,
    "dose_amt"::FLOAT as dose_amount,
    "dose_unit"::string as dose_unit,
    "dose_form"::string as dose_form,
    "dose_freq"::string as dose_frequency,
    "src_year"::int as source_year,
    "src_quarter"::int as source_quarter,

    from {{ source('ADVERSE_DRUGS', 'DRUG_FAERS') }}
)

-- SELECT * FROM drug_faers_init

SELECT 
    primary_id,
    demo.if_code,
    demo.event_date,
    demo.manufacturer_received_date,
    demo.initial_fda_received_date,
    demo.current_fda_received_date,
    demo.report_code,
    demo.manufacturer_number,
    demo.manufacturer_name,
    CASE 
        WHEN demo.age_in_years >= 0 AND demo.age_in_years <= 130 THEN demo.age_in_years -- Keep valid age values
        ELSE NULL -- For invalid age values (negative, extremely high, or null)
    END as age_in_years,
    demo.gender,
    demo.weight_in_lbs,
    drug.role_code,
    drug.drug_name,
    drug.is_validated_trade_name,
    drug.route,
    drug.dose,
    drug.cumulative_dose,
    drug.dechallenge,
    drug.rechallenge,
    drug.lot_number,
    drug.expiration_date,
    drug.nda_number,
    drug.dose_amount,
    drug.dose_unit,
    drug.dose_form,
    drug.dose_frequency,
    drug.source_year,
    drug.source_quarter
FROM demo_faers_init as demo
JOIN drug_faers_init as drug USING (primary_id)
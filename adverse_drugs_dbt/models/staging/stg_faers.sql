with 
demo_faers_init as (
    select 
    TRY_TO_NUMBER("primaryid"::string) as primary_id,
    "i_f_code"::string as if_code,
    TRY_TO_DATE("event_dt"::string, 'YYYY-MM-DD') as event_date,
    TRY_TO_DATE("mfr_dt"::string, 'YYYY-MM-DD') as manufacturer_received_date,
    TRY_TO_DATE("init_fda_dt"::string, 'YYYY-MM-DD') as initial_fda_received_date,
    TRY_TO_DATE("fda_dt"::string, 'YYYY-MM-DD') as current_fda_received_date,
    "rept_cod"::string as report_code,
    "mfr_num"::string as manufacturer_number,
    "mfr_sndr"::string as manufacturer_name,
    CASE 
        WHEN "age_cod" = 'DY' THEN (TRY_TO_NUMBER("age"::string) / 365.25)::int -- Convert days to years
        WHEN "age_cod" = 'MON' THEN (TRY_TO_NUMBER("age"::string) / 12)::int -- Convert months to years
        WHEN "age_cod" = 'YR' THEN (TRY_TO_NUMBER("age"::string))::int -- Already in years
        WHEN "age_cod" = 'DEC' THEN (TRY_TO_NUMBER("age"::string) * 10)::int -- Convert decades to years
        WHEN "age_cod" = 'WK' THEN (TRY_TO_NUMBER("age"::string) / 52.1775)::int -- Convert weeks to years
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
        WHEN "wt_cod" = 'KG' THEN ROUND(TRY_TO_NUMBER("wt"::string) * 2.20462, 1) -- Convert kilograms to pounds
        WHEN "wt_cod" = 'G' THEN ROUND(TRY_TO_NUMBER("wt"::string) / 1000 * 2.20462, 1) -- Convert grams to pounds
        WHEN "wt_cod" = 'LBS' THEN ROUND(TRY_TO_NUMBER("wt"::string), 1) -- Already in pounds
        ELSE NULL -- For unknown or other weight codes
    END as weight_in_lbs

    from {{ source('ADVERSE_DRUGS', 'DEMO_FAERS') }}
),

drug_faers_init as (
    select 
    TRY_TO_NUMBER(CONCAT("caseid"::string, "drug_seq"::string)::string) as primary_id,
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
    TRY_TO_DATE("exp_dt"::string, 'YYYY-MM-DD') as expiration_date,
    TRY_TO_NUMBER("nda_num"::string) as nda_number,
    "dose_amt"::string as dose_amt_raw,
    CASE
        WHEN "dose_amt" LIKE '%/%'
            THEN TRY_TO_NUMBER(SPLIT_PART("dose_amt"::string, '/', 1), 10, 2) / NULLIF(TRY_TO_NUMBER(SPLIT_PART("dose_amt"::string, '/', 2), 10, 2), 0) -- Extract the part after the '/' if it exists
        WHEN REGEXP_LIKE("dose_amt"::string, '^[0-9.]+\\s*[A-Za-z]+$')
            THEN TRY_TO_NUMBER(REGEXP_SUBSTR("dose_amt"::string, '^[0-9.]+'), 10, 2) -- Extract the numeric part if it's followed by letters (e.g., '5 mg')
        WHEN "dose_amt" LIKE '%-%' AND REGEXP_LIKE("dose_amt"::string, '^[0-9.]+\\-[0-9.]+$')
            THEN (TRY_TO_NUMBER(SPLIT_PART("dose_amt"::string, '-', 1), 10, 2) + TRY_TO_NUMBER(SPLIT_PART("dose_amt"::string, '-', 2), 10, 2)) / 2 -- Take the average of the two numbers if it's a range
        ELSE TRY_TO_NUMBER("dose_amt"::string, 10, 2) -- Use the original value if there is no '/'
    END as dose_amount,
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
    drug.dose_amt_raw,
    drug.dose_amount,
    drug.dose_unit,
    drug.dose_form,
    drug.dose_frequency,
    drug.source_year,
    drug.source_quarter
FROM demo_faers_init as demo
JOIN drug_faers_init as drug USING (primary_id)
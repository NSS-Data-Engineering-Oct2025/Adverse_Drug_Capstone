SELECT 
    faers.primary_id,
    faers.if_code,
    faers.event_date,
    faers.manufacturer_received_date,
    faers.initial_fda_received_date,
    faers.current_fda_received_date,
    faers.report_code,
    faers.manufacturer_number,
    faers.manufacturer_name,
    CASE 
        WHEN faers.age_in_years >= 0 AND faers.age_in_years <= 130 THEN faers.age_in_years -- Keep valid age values
        ELSE NULL -- For invalid age values (negative, extremely high, or null)
    END as age_in_years,
    faers.gender,
    faers.weight_in_lbs,
    faers.role_code,
    faers.drug_name,
    faers.is_validated_trade_name,
    CASE
        WHEN faers.route IS NULL THEN drugs.route
        ELSE faers.route
    END as route,
    faers.dose,
    faers.cumulative_dose,
    faers.dechallenge,
    faers.rechallenge,
    faers.lot_number,
    faers.expiration_date,
    faers.dose_amount,
    faers.dose_unit,
    faers.dose_form,
    faers.dose_frequency,
    faers.source_year,
    faers.source_quarter,
    drugs.last_updated,
    drugs.application_type,
    drugs.application_number,
    drugs.active_ingredient_name,
    drugs.active_ingredient_strength,
    drugs.brand_name,
    drugs.dosage_form,
    drugs.marketing_status,
    drugs.product_number,
    drugs.reference_drug,
    drugs.reference_standard,
    drugs.te_code,
    drugs.sponsor_name
FROM {{ref('stg_faers')}} AS faers
JOIN {{ref('stg_openfda_drugs')}} AS drugs
    ON faers.nda_number = drugs.application_number
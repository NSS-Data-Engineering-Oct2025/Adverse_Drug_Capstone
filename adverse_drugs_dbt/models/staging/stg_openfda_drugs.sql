with raw_source as (
    select "meta", "results" 
    from {{ source('ADVERSE_DRUGS', 'OPENFDA_DRUGS') }}
),

flattened_results as (
    select
        "meta":last_updated::timestamp as last_updated,

        -- Extract from the flattened 'results' variant
        CASE
            WHEN TRY_TO_NUMBER(RIGHT(r.value:application_number::string, 6)) IS NULL
                THEN LEFT(r.value:application_number::string, LEN(r.value:application_number::string) - 5) -- Handle cases where the last 6 characters are not numeric, extract the part of the string before the last 5 characters as application type
            ELSE LEFT(r.value:application_number::string, LEN(r.value:application_number::string) - 6) -- Extract the part of the string before the last 6 characters as application type
        END as application_type,
        CASE
            WHEN TRY_TO_NUMBER(RIGHT(r.value:application_number::string, 6)) IS NULL
                THEN TRY_TO_NUMBER(r.value:application_number::string, 5) -- Handle cases where the last 6 characters are not numeric, try 5 characters instead
            ELSE TRY_TO_NUMBER(RIGHT(r.value:application_number::string, 6)) -- Extract the last 6 characters and convert to number
        END as application_number,
        ai.value:name::string as active_ingredient_name,
        ai.value:strength::string as active_ingredient_strength,
        p.value:brand_name::string as brand_name,
        p.value:dosage_form::string as dosage_form,
        p.value:marketing_status::string as marketing_status,
        p.value:product_number::BIGINT as product_number,
        p.value:reference_drug::string as reference_drug,
        p.value:reference_standard::string as reference_standard,
        p.value:te_code::string as te_code,
        p.value:route::string as route,
        r.value:sponsor_name::string as sponsor_name,

    from raw_source,
    lateral flatten(input => "results") r,
    lateral flatten(input => r.value:products) p,
    lateral flatten(input => p.value:active_ingredients) ai
)

select * from flattened_results
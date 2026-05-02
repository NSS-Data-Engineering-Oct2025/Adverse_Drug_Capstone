with raw_source as (
    select "meta", "results" 
    from {{ source('ADVERSE_DRUGS', 'OPENFDA_DRUGS') }}
),

flattened_results as (
    select
        "meta":last_updated::timestamp as last_updated,

        -- Extract from the flattened 'results' variant
        r.value:application_number as application_number,
        ai.value:name as active_ingredient_name,
        ai.value:strength as active_ingredient_strength,
        p.value:brand_name as brand_name,
        p.value:dosage_form as dosage_form,
        p.value:marketing_status as marketing_status,
        p.value:product_number as product_number,
        p.value:reference_drug as reference_drug,
        p.value:reference_standard as reference_standard,
        p.value:te_code as te_code,
        p.value:route as route,
        r.value:sponsor_name as sponsor_name,

    from raw_source,
    lateral flatten(input => "results") r,
    lateral flatten(input => r.value:products) p,
    lateral flatten(input => p.value:active_ingredients) ai
)

select * from flattened_results
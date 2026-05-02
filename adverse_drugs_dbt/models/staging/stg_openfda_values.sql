-- with flattened_array as (
--     select
--         "meta":last_updated::timestamp as last_updated,
--         r.value as result_object
--     from {{ source('ADVERSE_DRUGS', 'OPENFDA_DRUGS') }},
--     lateral flatten(input => "results") r
-- )

-- select
--     last_updated,
--     f.key as field_name,
--     f.value as field_value
-- from flattened_array,
-- lateral flatten(input => result_object) f

with base_data as (
    select
        "meta":last_updated::timestamp as last_updated,
        r.value as result_object
    from {{ source('ADVERSE_DRUGS', 'OPENFDA_DRUGS') }},
    lateral flatten(input => "results") r
)

select
    result_object:reportnumber::string as report_id,
    v.key as field_name,
    v.value as field_value
from base_data,
lateral flatten(input => result_object:submissions) s,
lateral flatten(input => s.value) v                     
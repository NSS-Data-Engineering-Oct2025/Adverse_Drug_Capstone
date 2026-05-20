select
    age_in_years
from {{ ref('stg_faers') }}
WHERE age_in_years < 0 OR age_in_years > 130
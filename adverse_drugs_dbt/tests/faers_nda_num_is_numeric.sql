select
    nda_number,
from {{ ref('stg_faers') }}
WHERE TRY_TO_NUMBER(nda_number) IS NULL
    AND nda_number IS NOT NULL
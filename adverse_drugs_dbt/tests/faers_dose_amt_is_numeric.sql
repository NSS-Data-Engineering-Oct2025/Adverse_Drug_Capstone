select
    dose_amount,
    dose_amt_raw
from {{ ref('stg_faers') }}
where dose_amount is null
    and dose_amt_raw is not null -- Checks if it was a real value that failed to parse
    and not regexp_like(dose_amt_raw, '^[A-Za-z ]+$') -- Ignore values that consist entirely of letters and spaces (e.g., 'UNKNOWN', 'variable')

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    initcap(trim(customer_city)) as city,
    upper(trim(customer_state)) as state
from {{ source('raw', 'customers') }}

select order_id
from {{ ref('fact_orders') }}
where delivery_days < 0

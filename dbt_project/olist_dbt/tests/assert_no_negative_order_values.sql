select order_id
from {{ ref('fact_orders') }}
where items_value < 0 or freight_value < 0 or payment_value < 0

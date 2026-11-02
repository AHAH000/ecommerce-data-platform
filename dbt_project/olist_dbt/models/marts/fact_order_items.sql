select
    oi.order_item_key,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.purchased_at::date as purchase_date,
    o.order_status,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value as total_value
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }} o using (order_id)

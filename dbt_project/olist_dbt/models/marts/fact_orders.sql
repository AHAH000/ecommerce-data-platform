with items as (
    select
        order_id,
        count(*) as item_count,
        sum(price) as items_value,
        sum(freight_value) as freight_value
    from {{ ref('stg_order_items') }}
    group by 1
),
payments as (
    select
        order_id,
        sum(payment_value) as payment_value
    from {{ ref('stg_order_payments') }}
    group by 1
),
reviews as (
    select
        order_id,
        avg(review_score) as review_score
    from {{ ref('stg_order_reviews') }}
    group by 1
)
select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchased_at,
    o.purchased_at::date as purchase_date,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,
    coalesce(i.item_count, 0) as item_count,
    coalesce(i.items_value, 0) as items_value,
    coalesce(i.freight_value, 0) as freight_value,
    coalesce(p.payment_value, 0) as payment_value,
    r.review_score,
    (o.delivered_to_customer_at::date - o.purchased_at::date) as delivery_days,
    (o.delivered_to_customer_at::date - o.estimated_delivery_at::date) as days_vs_estimate
from {{ ref('stg_orders') }} o
left join items i using (order_id)
left join payments p using (order_id)
left join reviews r using (order_id)

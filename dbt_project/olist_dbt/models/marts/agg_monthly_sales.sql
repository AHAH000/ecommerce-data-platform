select
    date_trunc('month', purchase_date)::date as month,
    count(*) as orders,
    sum(items_value) as items_revenue,
    sum(freight_value) as freight_revenue,
    round(avg(items_value), 2) as avg_order_value,
    round(avg(review_score), 2) as avg_review_score,
    round(avg(delivery_days), 1) as avg_delivery_days
from {{ ref('fact_orders') }}
where order_status <> 'canceled'
group by 1
order by 1

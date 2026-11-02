select
    p.product_id,
    p.category_name,
    coalesce(t.category_name_english, p.category_name, 'unknown') as category_name_english,
    p.weight_g,
    p.length_cm,
    p.height_cm,
    p.width_cm
from {{ ref('stg_products') }} p
left join {{ ref('stg_category_translation') }} t
    on p.category_name = t.category_name

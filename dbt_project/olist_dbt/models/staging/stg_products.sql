select
    product_id,
    nullif(trim(product_category_name), '') as category_name,
    nullif(product_name_lenght, '')::numeric::int as name_length,
    nullif(product_description_lenght, '')::numeric::int as description_length,
    nullif(product_photos_qty, '')::numeric::int as photos_qty,
    nullif(product_weight_g, '')::numeric as weight_g,
    nullif(product_length_cm, '')::numeric as length_cm,
    nullif(product_height_cm, '')::numeric as height_cm,
    nullif(product_width_cm, '')::numeric as width_cm
from {{ source('raw', 'products') }}

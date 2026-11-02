select
    trim(product_category_name) as category_name,
    trim(product_category_name_english) as category_name_english
from {{ source('raw', 'category_translation') }}

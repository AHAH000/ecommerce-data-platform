select
    review_id || '-' || order_id as review_key,
    review_id,
    order_id,
    review_score::int as review_score,
    nullif(review_comment_title, '') as comment_title,
    nullif(review_comment_message, '') as comment_message,
    nullif(review_creation_date, '')::timestamp as created_at,
    nullif(review_answer_timestamp, '')::timestamp as answered_at,
    _purchase_date
from {{ source('raw', 'order_reviews') }}

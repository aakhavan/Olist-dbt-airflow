select
    review_id,
    order_id,
    toInt64OrNull(review_score) as review_score,
    review_comment_title,
    review_comment_message,
    parseDateTimeBestEffortOrNull(review_creation_date) as review_creation_date,
    parseDateTimeBestEffortOrNull(review_answer_timestamp) as review_answer_timestamp

from {{ source('olist_raw', 'olist_order_reviews_dataset') }}
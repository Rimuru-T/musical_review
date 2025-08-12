select
    reviewerID,
    asin,
    reviewerName,
    helpful,
    reviewText,
    overall,
    summary,
    unixReviewTime,
    reviewTime
from {{ source('my_postgres_source', 'staging_reviews') }}

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
from "music_reviews"."raw"."staging_reviews"
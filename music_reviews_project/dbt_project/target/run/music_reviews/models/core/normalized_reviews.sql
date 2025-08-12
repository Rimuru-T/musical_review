
  
    

  create  table "music_reviews"."raw"."normalized_reviews__dbt_tmp"
  
  
    as
  
  (
    with base as (
    select * from "music_reviews"."raw"."staging_reviews"
),

grades as (
    select distinct
        asin,
        helpful,
        reviewText,
        overall
    from base
),

customers as (
    select distinct
        reviewerID,
        reviewerName
    from base
),

reviews as (
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
    from base
)

select
    r.reviewerID,
    r.asin,
    r.reviewerName,
    r.helpful,
    r.reviewText,
    r.overall,
    r.summary,
    r.unixReviewTime,
    r.reviewTime
from reviews r
  );
  
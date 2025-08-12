
  create view "music_reviews"."raw"."reviews_summary__dbt_tmp"
    
    
  as (
    select
    asin,
    count(*) as total_reviews,
    avg(overall) as avg_overall
    
from "music_reviews"."raw"."normalized_reviews"
group by asin
  );
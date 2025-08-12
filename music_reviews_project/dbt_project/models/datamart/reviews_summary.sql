select
    asin,
    count(*) as total_reviews,
    avg(overall) as avg_overall
    
from {{ ref('normalized_reviews') }}
group by asin

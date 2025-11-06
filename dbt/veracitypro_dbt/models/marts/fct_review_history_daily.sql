{{ config(materialized='table', unique_key='pk') }}
select md5(asin||'|'||to_varchar(date)) as pk,
       asin, date, rating, review_count
from {{ ref('core_keepa_reviews_daily') }};

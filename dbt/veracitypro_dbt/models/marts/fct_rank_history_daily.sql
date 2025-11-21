{{ config(materialized='table') }}

select
    md5(asin || '|' || to_varchar(date)) as pk,
    asin,
    date,
    rank
from {{ ref('core_keepa_rank_daily') }}
{{ config(materialized='table', unique_key='pk') }}
select md5(asin||'|'||price_channel||'|'||to_varchar(date)) as pk,
       asin, price_channel, date, price
from {{ ref('core_keepa_price_daily') }};
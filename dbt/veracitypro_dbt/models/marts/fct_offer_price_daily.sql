{{ config(materialized='table', unique_key='pk') }}
select md5(asin||'|'||offer_id||'|'||to_varchar(date)) as pk,
       asin, offer_id, date, price
from {{ ref('core_keepa_offer_price_daily') }};

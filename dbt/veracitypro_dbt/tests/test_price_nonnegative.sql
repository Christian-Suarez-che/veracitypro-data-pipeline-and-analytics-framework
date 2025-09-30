-- Fails if a negative price slips in
select *
from {{ ref('stg_keepa_price') }}
where price_usd < 0

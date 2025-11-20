-- tests/test_price_nonnegative.sql
-- Fails if a negative price slips in
select *
from {{ ref('stg_keepa_price') }}
where price < 0

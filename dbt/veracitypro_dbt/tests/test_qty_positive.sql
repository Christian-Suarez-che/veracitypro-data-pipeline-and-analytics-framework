-- Fails if quantity is null or <= 0
select *
from {{ ref('stg_spapi_orders') }}
where qty is null or qty <= 0

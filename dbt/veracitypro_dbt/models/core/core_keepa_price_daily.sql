-- Summary:
-- - Daily last value per (asin, price_channel).
-- - Stable panel for analytics and marts (fct_price_history_daily).
{{ config(materialized='incremental', unique_key='pk', incremental_strategy='merge') }}

with src as (
  select asin, price_channel, snapshot_ts::date as date, price, ingest_ts
  from {{ ref('stg_keepa_price') }}
  {% if is_incremental() %} where snapshot_ts::date > (select coalesce(max(date),'1900-01-01') from {{ this }}) {% endif %}
),
dedup as (
  select *
  from (
    select s.*, row_number() over (partition by asin, price_channel, date order by ingest_ts desc, snapshot_ts desc) rn
    from src s
  ) where rn = 1
)
select md5(asin||'|'||price_channel||'|'||to_varchar(date)) as pk,
       asin, price_channel, date, price, ingest_ts
from dedup;

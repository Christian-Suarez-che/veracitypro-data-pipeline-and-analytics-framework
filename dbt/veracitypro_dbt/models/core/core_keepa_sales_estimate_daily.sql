-- models/core/core_keepa_sales_estimate_daily.sql
{{ config(materialized='table') }}

with ranked as (
  select
    *,
    row_number() over (
      partition by asin
      order by ingest_ts desc
    ) as rn
  from {{ ref('stg_keepa_sales_stats') }}
),

latest as (
  select
    asin,
    sold_last_30d,
    sold_last_90d
  from ranked
  where rn = 1
)

select
  asin,
  sold_last_30d,
  sold_last_90d,
  sold_last_30d / 30.0 as est_units_per_day_30d,
  sold_last_90d / 90.0 as est_units_per_day_90d
from latest
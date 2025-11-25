-- models/core/core_keepa_buybox_daily.sql
{{ config(materialized='table') }}

with base as (
  select
    asin,
    snapshot_ts::date as date,
    seller_id
  from {{ ref('stg_keepa_buybox_history') }}
),

ranked as (
  select
    asin,
    date,
    seller_id,
    row_number() over (
      partition by asin, date
      order by date, seller_id
    ) as rn
  from base
)

select
  asin,
  date,
  seller_id
from ranked
where rn = 1
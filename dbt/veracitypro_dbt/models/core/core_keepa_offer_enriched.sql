-- models/core/core_keepa_offer_enriched.sql
{{ config(materialized='table') }}

with price as (
  select
    asin,
    offer_id,
    snapshot_ts::date as date,
    price
  from {{ ref('stg_keepa_offer_price') }}
),

meta as (
  select
    asin,
    offer_id,
    seller_id,
    is_prime,
    is_map,
    condition_code,
    shipping_dollars
  from {{ ref('stg_keepa_offer_metadata') }}
  qualify row_number() over (
    partition by asin, offer_id
    order by ingest_ts desc
  ) = 1
)

select
  p.asin,
  p.offer_id,
  p.date,
  p.price,
  m.seller_id,
  m.is_prime,
  m.is_map,
  m.condition_code,
  m.shipping_dollars
from price p
left join meta m
  on p.asin = m.asin
 and p.offer_id = m.offer_id
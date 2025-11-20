-- models/stg/stg_keepa_offers.sql
-- Summary:
-- - Static attributes for each offer (offerId) and raw offerCSV kept for audit.
-- - Downstream: stg_keepa_offer_price parses the CSV to time-series.
{{ config(materialized='incremental', unique_key='stg_offer_id', incremental_strategy='merge', tags=['keepa','offers']) }}

with base as (
  select
    offers_v,          -- offers array from _stg_keepa_base
    asin,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
),
offers as (
  select
    b.asin,
    o.value:offerId::string      as offer_id,
    o.value:sellerId::string     as seller_id,
    o.value:isPrime::boolean     as is_prime,
    o.value:isFBA::boolean       as is_fba,
    o.value:condition::string    as condition,
    o.value:isShippable::boolean as is_shippable,
    o.value:isMAP::boolean       as is_map,
    o.value:offerCSV::string     as offer_csv_raw,
    b.ingest_ts
  from base b,
       lateral flatten(input => b.offers_v, outer => true) o
),
dedup as (
  select
    md5(asin||'|'||offer_id) as stg_offer_id,
    *
  from (
    select o.*, row_number() over (partition by asin, offer_id order by ingest_ts desc) rn
    from offers o
  ) where rn = 1
)
select * from dedup

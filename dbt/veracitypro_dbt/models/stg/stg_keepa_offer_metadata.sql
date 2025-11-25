-- models/stg/stg_keepa_offer_metadata.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_offer_meta_id',
    incremental_strategy = 'merge',
    tags = ['keepa','offers','metadata']
) }}

with base as (
  select
    asin,
    offers_v,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
  where offers_v is not null
),

flattened as (
  select
    b.asin,
    off.value:offerId::number   as offer_id,
    off.value:sellerId::string  as seller_id,
    off.value:isPrime::boolean  as is_prime,
    off.value:isMAP::boolean    as is_map,
    off.value:condition::string as condition_code,
    off.value:shipping::number  as shipping_cents,
    b.ingest_ts
  from base b,
       lateral flatten(input => b.offers_v) off
),

dedup as (
  select
    md5(asin || '|' || to_varchar(offer_id) || '|' || to_varchar(ingest_ts)) as stg_offer_meta_id,
    asin,
    offer_id,
    seller_id,
    is_prime,
    is_map,
    condition_code,
    case when shipping_cents is null then null else shipping_cents / 100.0 end as shipping_dollars,
    ingest_ts
  from flattened
)

select *
from dedup
{% if is_incremental() %}
where ingest_ts > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
{% endif %}
-- models/stg/stg_keepa_buybox_history.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_buybox_row_id',
    incremental_strategy = 'merge',
    tags = ['keepa','buybox']
) }}

with base as (
  select
    asin,
    buybox_history_v,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
  where buybox_history_v is not null
),

-- Interpret buyBoxSellerIdHistory as CSV: minute, sellerId, minute, sellerId, ...
tokens as (
  select
    b.asin,
    b.ingest_ts,
    f.index            as i,
    f.value::string    as token
  from base b,
       lateral flatten(input => split(b.buybox_history_v::string, ',')) f
),

paired as (
  select
    i.asin,
    try_to_number(i.token) as keepa_min,
    p.token                as seller_id_raw,
    i.ingest_ts
  from tokens i
  join tokens p
    on p.asin      = i.asin
   and p.ingest_ts = i.ingest_ts
   and p.i        = i.i + 1
  where mod(i.i, 2) = 0
),

clean as (
  select
    asin,
    to_timestamp_ntz( (keepa_min + 21564000) * 60 ) as snapshot_ts,
    nullif(seller_id_raw, '')::string               as seller_id,
    ingest_ts
  from paired
),

dedup as (
  select
    md5(
      asin || '|' ||
      to_varchar(snapshot_ts) || '|' ||
      coalesce(seller_id,'NULL')
    ) as stg_buybox_row_id,
    asin,
    snapshot_ts,
    seller_id,
    ingest_ts
  from (
    select
      c.*,
      row_number() over (
        partition by asin, snapshot_ts
        order by ingest_ts desc
      ) as rn
    from clean c
  )
  where rn = 1
)

select *
from dedup
{% if is_incremental() %}
where ingest_ts > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
{% endif %}
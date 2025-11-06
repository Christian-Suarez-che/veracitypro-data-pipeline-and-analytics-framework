-- models/stg/stg_keepa_price.sql
{{ config(materialized='incremental', unique_key='pk', incremental_strategy='merge') }}

with base as (select asin, csv_v, ingest_ts from {{ ref('_stg_keepa_base') }}),

-- helper: explode a channel array of [min,val,min,val,...] into rows
channel as (
  select
    b.asin,
    ch.channel_name,
    ch.chan_idx,
    seq4() as pos,
    (b.csv_v[ch.chan_idx][2*pos])::number        as keepa_min,
    (b.csv_v[ch.chan_idx][2*pos+1])::number      as raw_val,
    b.ingest_ts
  from base b,
  lateral ( select column1::string as channel_name, column2::number as chan_idx
            from values ('AMAZON',0),('NEW',1),('USED',2),('LISTPRICE',4) ) ch
  qualify 2*pos+1 < array_size(b.csv_v[ch.chan_idx])
),
normalized as (
  select
    asin,
    channel_name as price_channel,
    -- Keepa minutes since 2011-01-01 -> TIMESTAMP
    to_timestamp_ntz( (keepa_min + 21564000) * 60 ) as snapshot_ts,
    iff(raw_val < 0, null, raw_val/100.0) as price,          -- cents→$
    iff(raw_val < 0, 1, 0)                 as was_sentinel,
    ingest_ts
  from channel
)
select
  md5(asin||'|'||price_channel||'|'||to_varchar(snapshot_ts)) as pk,
  asin, price_channel, snapshot_ts, price, was_sentinel, ingest_ts
from normalized
{% if is_incremental() %}
where snapshot_ts::date >= (select coalesce(max(snapshot_ts::date),'1900-01-01') from {{ this }})
{% endif %};
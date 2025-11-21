-- models/stg/stg_keepa_offer_price.sql
-- Summary:
-- - Parses offerCSV as sequence of minute,price_cents pairs for each offer.
-- - Emits (asin, offer_id, snapshot_ts, price, was_sentinel, ingest_ts).

{{ config(
    materialized='incremental',
    unique_key='stg_offer_price_id',
    incremental_strategy='merge',
    tags=['keepa','offers','price']
) }}

with base as (
    select
        asin,
        offer_id,
        offer_csv_raw,
        ingest_ts
    from {{ ref('stg_keepa_offers') }}
    where offer_csv_raw is not null
      and offer_csv_raw <> ''
    {% if is_incremental() %}
      and ingest_ts > (
          select coalesce(max(ingest_ts), '1900-01-01')
          from {{ this }}
      )
    {% endif %}
),

tokens as (
    -- Split CSV string into array tokens and enumerate them
    select
        b.asin,
        b.offer_id,
        b.ingest_ts,
        f.index              as i,    -- token index
        f.value::string      as t     -- token value
    from base as b,
         lateral flatten(input => split(b.offer_csv_raw, ',')) as f
),

paired as (
    -- Pair i (minute), i+1 (price_cents)
    -- 2011-01-01 00:00:00 UTC = UNIX 1293840000.
    -- Keepa minutes -> seconds -> timestamp (NTZ to avoid TZ drift at rest).
    select
        i.asin,
        i.offer_id,
        to_timestamp_ntz( (try_to_number(i.t) + 21564000) * 60 ) as snapshot_ts,
        try_to_number(p.t) / 100.0                               as price_dollars_raw,
        i.ingest_ts
    from tokens as i
    join tokens as p
      on p.asin      = i.asin
     and p.offer_id  = i.offer_id
     and p.ingest_ts = i.ingest_ts
     and p.i         = i.i + 1
    where mod(i.i, 2) = 0
),

clean as (
    select
        asin,
        offer_id,
        snapshot_ts,
        case when price_dollars_raw < 0 then null else price_dollars_raw end as price,
        case when price_dollars_raw < 0 then 1    else 0               end as was_sentinel,
        ingest_ts
    from paired
),

dedup as (
    select
        md5(asin || '|' || offer_id || '|' || to_varchar(snapshot_ts)) as stg_offer_price_id,
        *
    from (
        select
            c.*,
            row_number() over (
                partition by asin, offer_id, snapshot_ts
                order by ingest_ts desc
            ) as rn
        from clean c
    )
    where rn = 1
)

select *
from dedup
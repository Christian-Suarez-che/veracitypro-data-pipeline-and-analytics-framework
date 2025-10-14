{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_id',
    incremental_strategy = 'merge',
    tags = ['stg','keepa']
) }}

-- Source rows (tolerant to naming differences: ingest_dt vs ingest_ts, filename vs metadata$filename)
with src as (
  select
    payload,
    /* prefer ingest_ts; fall back to ingest_dt if that's what RAW has */
    coalesce(ingest_ts, ingest_dt)::timestamp_ntz as ingest_ts,
    /* try filename columns that COPY INTO often exposes; else NULL */
    coalesce(filename, metadata$filename)::string as filename,
    /* optional snapshot date for downstream partitioning */
    to_date(coalesce(ingest_ts, ingest_dt)) as snapshot_date,
    payload:"asin"::string as asin
  from {{ source('raw','KEEPA_RAW') }}
  {% if is_incremental() %}
    -- Only scan new loads on incremental runs
    where coalesce(ingest_ts, ingest_dt) >
          (select coalesce(max(ingest_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

-- Expand priceEvents array and normalize timestamps & numeric types
events as (
  select
    s.asin,

    /* event_ts can be ISO string or epoch (s/ms) */
    case
      when try_to_timestamp((e.value:"ts")::string) is not null
        then try_to_timestamp((e.value:"ts")::string)
      when try_to_number((e.value:"ts")::string) >= 100000000000  -- >= 1e11 → likely ms
        then to_timestamp_ntz( try_to_number((e.value:"ts")::string) / 1000 )
      when try_to_number((e.value:"ts")::string) is not null
        then to_timestamp_ntz( try_to_number((e.value:"ts")::string) )
      else null
    end as event_ts,

    try_to_number((e.value:"price")::string) as price_cents,

    s.filename,
    s.ingest_ts,
    s.snapshot_date
  from src s,
  lateral flatten(input => s.payload:"priceEvents") e
),

-- Shape for validation
base as (
  select
    asin,
    event_ts,
    case when price_cents is not null then price_cents / 100.0 end as price,  -- USD
    filename,
    ingest_ts,
    snapshot_date
  from events
),

-- Filter obviously bad records
valid as (
  select *
  from base
  where asin is not null
    and event_ts is not null
    and price is not null
    and price >= 0
),

-- Deduplicate to the newest record per (asin, event_ts)
dedup as (
  select
    md5(asin || '|' || to_varchar(event_ts) || '|' || coalesce(filename,'NULL')) as stg_id,
    asin,
    event_ts,
    price,
    filename,
    ingest_ts,
    snapshot_date
  from (
    select
      v.*,
      row_number() over (partition by asin, event_ts order by ingest_ts desc nulls last) as rn
    from valid v
  )
  where rn = 1
)

select * from dedup;
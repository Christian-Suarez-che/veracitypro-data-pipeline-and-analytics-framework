{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_id',
    incremental_strategy = 'merge',
    tags = ['stg','scraper']
) }}

-- 1) Raw source with robust fallbacks and incremental prune
with src as (
  select
    payload,

    -- Prefer ingest_ts; fall back to ingest_dt if RAW uses that
    coalesce(ingest_ts, ingest_dt)::timestamp_ntz as ingest_ts,

    -- COPY INTO often exposes filename or metadata$filename
    coalesce(filename, metadata$filename)::string as filename
  from {{ source('raw','SCRAPERAPI_RAW') }}
  {% if is_incremental() %}
    where coalesce(ingest_ts, ingest_dt) >
          (select coalesce(max(ingest_ts),'1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

-- 2) Normalize + extract fields from JSON
base as (
  select
    /* core identity */
    upper(regexp_replace(trim(payload:"asin"::string), '\\s+', ''))                           as asin,
    regexp_replace(trim(payload:"title"::string), '\\s+', ' ')                                as title,
    regexp_replace(trim(payload:"brand"::string), '\\s+', ' ')                                as brand,
    regexp_replace(trim(payload:"category"::string), '\\s+', ' ')                             as category,

    /* prices (best-effort) */
    try_to_number((payload:"price")::string)                                                  as price,
    try_to_number((payload:"list_price")::string)                                             as list_price,

    /* coupon / promo flags (coerce to boolean-ish) */
    case
      when lower((payload:"coupon"::string)) in ('true','t','1','yes','y') then true
      when try_to_number((payload:"coupon")::string) = 1 then true
      else false
    end                                                                                       as coupon_flag,

    /* event_ts can be ISO string, numeric epoch s/ms, or missing */
    case
      when try_to_timestamp((payload:"scraped_at")::string) is not null
        then try_to_timestamp((payload:"scraped_at")::string)
      when try_to_number((payload:"scraped_at")::string) >= 100000000000
        then to_timestamp_ntz( try_to_number((payload:"scraped_at")::string) / 1000 )
      when try_to_number((payload:"scraped_at")::string) is not null
        then to_timestamp_ntz( try_to_number((payload:"scraped_at")::string) )
      else null
    end                                                                                       as event_ts_raw,

    /* carry-through metadata */
    filename,
    ingest_ts
  from src
),

-- 3) Final shape + safeguards
shaped as (
  select
    asin,
    title,
    brand,
    category,

    /* ensure non-negative, NULL if bad */
    case when price      is not null and price      >= 0 then price      end as price,
    case when list_price is not null and list_price >= 0 then list_price end as list_price,

    coupon_flag,

    /* if event_ts missing, fall back to ingest_ts so we can still build timelines */
    coalesce(event_ts_raw, ingest_ts) as event_ts,

    /* partition helper */
    to_date(ingest_ts) as snapshot_date,

    filename,
    ingest_ts
  from base
),

-- 4) Validate rows we keep
valid as (
  select *
  from shaped
  where asin is not null
    and event_ts is not null
    and price is not null
    and price >= 0
),

-- 5) Deduplicate newest per (asin, event_ts)
dedup as (
  select
    md5(asin || '|' || to_varchar(event_ts) || '|' || coalesce(filename,'NULL')) as stg_id,
    asin, title, brand, category,
    price, list_price, coupon_flag,
    event_ts, snapshot_date, filename, ingest_ts
  from (
    select
      v.*,
      row_number() over (partition by asin, event_ts order by ingest_ts desc nulls last) as rn
    from valid v
  )
  where rn = 1
)

select * from dedup;

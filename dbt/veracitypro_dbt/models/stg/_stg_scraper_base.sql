{{ config(materialized='view') }}

{% set bounds = vp_prev_day_bounds(var('as_of_date', current_date()|string)) %}

with raw as (
  select
    payload,
    ingest_ts,
    metadata$filename::string as filename
  from RAW.SCRAPER_PRODUCT_RAW_STRM   -- or SCRAPERAPI_RAW_STRM if that's your table
  where ingest_ts between {{ bounds['start'] }} and {{ bounds['end'] }}
),
norm as (
  select
    upper(regexp_replace(trim(payload:'ASIN'::string), '\\s+', ''))          as asin,
    regexp_replace(trim(payload:'title_text'::string), '\\s+', ' ')          as title,
    regexp_replace(trim(payload:'Brand'::string), '\\s+', ' ')               as brand,
    try_to_number(trim(payload:'price'::string))                              as price,
    try_to_number(trim(payload:'listPrice'::string))                          as list_price,
    iff(lower(trim(payload:'couponFlag'::string))='true', true, false)        as coupon_flag,
    try_to_timestamp_ntz(payload:'snapshot_utc'::string)                      as event_ts,
    filename,
    ingest_ts,
    to_date({{ bounds['start'] }})                                            as snapshot_date
  from raw
)
select * from norm

{{ config(materialized='view') }}

{# Compute yesterday window in Eastern #}
{% set bounds = vp_prev_day_bounds(var('as_of_date', current_date()|string)) %}

with raw as (
  select
    payload,
    ingest_ts,
    metadata$filename::string as filename
  from RAW.KEEPA_RAW_STRM
  where ingest_ts between {{ bounds['start'] }} and {{ bounds['end'] }}
),
norm as (
  select
    upper(regexp_replace(trim(payload:'asin'::string), '\\s+', ''))     as asin,
    try_to_timestamp_ntz(payload:'ts'::string)                           as event_ts,
    payload:'price'::float                                              as price,          -- nullable; event-specific
    payload:'rank'::float                                               as rank_value,     -- nullable; event-specific
    payload:'rating'::float                                             as rating_value,   -- nullable; event-specific
    filename,
    ingest_ts,
    /* convenience day for rollups */
    to_date({{ bounds['start'] }})                                      as snapshot_date
  from raw
)
select * from norm

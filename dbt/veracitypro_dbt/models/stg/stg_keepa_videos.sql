-- models/stg/stg_keepa_videos.sql
-- Summary:
-- - Flattens videos[] into stable rows (asin, url/name) with metadata.
{{ config(materialized='incremental', unique_key='stg_video_id', incremental_strategy='merge', tags=['keepa','media']) }}

with base as (select product_v, asin, ingest_ts from {{ ref('_stg_keepa_base') }}),
vid as (
  select
    b.asin,
    v.value:creator::string  as creator,
    v.value:duration::number as duration_sec,
    v.value:image::string    as thumbnail_url,
    v.value:name::string     as name,
    v.value:title::string    as title,
    v.value:url::string      as url,
    b.ingest_ts
  from base b, lateral flatten(input => b.product_v:videos, outer => true) v
),
dedup as (
  select
    md5(asin||'|'||coalesce(url, name)) as stg_video_id,
    *
  from (
    select v.*, row_number() over (partition by asin, coalesce(url,name) order by ingest_ts desc) rn
    from vid v
  ) where rn = 1
)
select * from dedup;

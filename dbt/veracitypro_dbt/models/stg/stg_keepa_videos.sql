{{ config(
    materialized = 'incremental',
    unique_key = 'stg_video_id',
    incremental_strategy = 'merge',
    tags = ['keepa', 'media']
) }}

with base as (
    select
        VIDEOS_V as product_v,   -- this is the actual videos array
        ASIN,
        INGEST_TS as snapshot_ts
    from {{ ref('_stg_keepa_base') }}

    {% if is_incremental() %}
    where INGEST_TS::date >= (
        select coalesce(max(snapshot_ts::date), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),

vid as (
    select
        b.asin,
        b.snapshot_ts,
        v.value:creator::string  as creator,
        iff(v.value:duration::number < 0, null, v.value:duration::number) as duration_sec,
        v.value:image::string    as thumbnail_url,
        v.value:name::string     as name,
        v.value:title::string    as title,
        v.value:url::string      as url
    from base b,
         lateral flatten(input => b.product_v, outer => true) v
),

dedup as (
    select
        md5(
            asin || '|' ||
            coalesce(url, name) || '|' ||
            to_char(snapshot_ts, 'YYYY-MM-DD"T"HH24:MI:SSFF3')
        ) as stg_video_id,
        d.*
    from (
        select
            v.*,
            row_number() over (
                partition by asin, coalesce(url, name)
                order by snapshot_ts desc
            ) as rn
        from vid v
    ) d
    where rn = 1
      and (url is not null or name is not null)  -- filter out non-video rows
)

select
    stg_video_id,
    asin,
    snapshot_ts,
    creator,
    duration_sec,
    thumbnail_url,
    name,
    title,
    url
from dedup
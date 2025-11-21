{{ config(
    materialized='incremental',
    unique_key='pk',
    incremental_strategy='merge'
) }}

with src as (
    select
        asin,
        snapshot_ts,                     -- keep full timestamp
        snapshot_ts::date as date,       -- date version
        rating,
        review_count,
        ingest_ts
    from {{ ref('stg_keepa_reviews') }}
    {% if is_incremental() %}
    where snapshot_ts::date > (
        select coalesce(max(date), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
),

dedup as (
    select *
    from (
        select
            s.*,
            row_number() over (
                partition by asin, date
                order by ingest_ts desc, snapshot_ts desc
            ) as rn
        from src as s
    )
    where rn = 1
)

select
    md5(asin || '|' || to_varchar(date)) as pk,
    asin,
    date,
    rating,
    review_count,
    ingest_ts
from dedup
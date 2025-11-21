{{ config(
    materialized='incremental',
    unique_key='pk',
    incremental_strategy='merge'
) }}

with src as (
    select
        asin,
        offer_id,
        snapshot_ts,                  -- keep full timestamp
        snapshot_ts::date as date,    -- daily grain
        price,
        ingest_ts
    from {{ ref('stg_keepa_offer_price') }}
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
                partition by asin, offer_id, date
                order by ingest_ts desc, snapshot_ts desc
            ) as rn
        from src as s
    )
    where rn = 1
)

select
    md5(asin || '|' || offer_id || '|' || to_varchar(date)) as pk,
    asin,
    offer_id,
    date,
    price,
    ingest_ts
from dedup
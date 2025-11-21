-- models/stg/stg_keepa_price__bad.sql
-- Summary:
-- - Captures truly bad rows from price parsing (missing asin/ts, non-numeric).
-- - Keepa sentinels (-1) are NOT bad; they are mapped to NULL in good table.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='bad_row_id',
    schema='STG_BAD',
    tags=['quarantine','keepa','price']
) }}

with good as (
    select
        asin,
        price_channel,
        snapshot_ts,
        price,
        was_sentinel,
        ingest_ts
    from {{ ref('stg_keepa_price') }}
),

bad as (
    select
        md5(
            coalesce(asin, 'NULL') || '|' ||
            coalesce(price_channel, 'NULL') || '|' ||
            coalesce(to_varchar(snapshot_ts), 'NULL') || '|' ||
            coalesce(to_varchar(ingest_ts), 'NULL')
        ) as bad_row_id,
        asin,
        price_channel,
        snapshot_ts,
        price,
        was_sentinel,
        ingest_ts,
        case
            when asin is null then 'ASIN_NULL'
            when snapshot_ts is null then 'TS_NULL'
            when price is not null and price < 0 then 'PRICE_NEGATIVE'
            else 'UNKNOWN'
        end as reason_code,
        'Price staging validation failure' as reason_detail
    from good
    where
        asin is null
        or snapshot_ts is null
        or (price is not null and price < 0)
)

select *
from bad
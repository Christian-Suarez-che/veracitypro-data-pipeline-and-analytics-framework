{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='bad_row_id',
    schema='STG_BAD',
    tags=['quarantine','keepa','rank']
) }}

with good as (
    select
        asin,
        snapshot_ts,
        rank,
        ingest_ts
    from {{ ref('stg_keepa_rank') }}
),

bad as (
    select
        md5(
            coalesce(asin,'NULL') || '|' ||
            coalesce(to_varchar(snapshot_ts),'NULL') || '|' ||
            coalesce(to_varchar(ingest_ts),'NULL')
        ) as bad_row_id,
        asin,
        snapshot_ts,
        rank,
        ingest_ts,
        case
            when asin is null then 'ASIN_NULL'
            when snapshot_ts is null then 'TS_NULL'
            when rank is not null and rank <= 0 then 'RANK_NONPOSITIVE'
            else 'UNKNOWN'
        end as reason_code,
        'Rank staging validation failure' as reason_detail
    from good
    where
        asin is null
        or snapshot_ts is null
        or (rank is not null and rank <= 0)
)

select *
from bad
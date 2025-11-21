{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='bad_row_id',
    schema='STG_BAD',
    tags=['quarantine','keepa','aplus']
) }}

with src as (
    select
        asin,
        aplus_idx,
        module_idx,
        asset_type,
        asset_idx,
        url,
        alt_text,
        text,
        ingest_ts
    from {{ ref('stg_keepa_aplus_module') }}
),

agg as (
    select
        asin,
        aplus_idx,
        module_idx,
        count(*)        as asset_count,
        max(ingest_ts)  as ingest_ts
    from src
    group by 1,2,3
),

bad as (
    select
        md5(
            coalesce(asin,'NULL') || '|' ||
            coalesce(to_varchar(aplus_idx),'NULL') || '|' ||
            coalesce(to_varchar(module_idx),'NULL') || '|' ||
            coalesce(to_varchar(ingest_ts),'NULL')
        ) as bad_row_id,
        asin,
        aplus_idx,
        module_idx,
        asset_count,
        ingest_ts,
        case
            when asin is null     then 'ASIN_NULL'
            when asset_count = 0  then 'MODULE_EMPTY'
            else 'UNKNOWN'
        end as reason_code,
        'A+ module empty or asin missing' as reason_detail
    from agg
    where asin is null or asset_count = 0
)

select *
from bad

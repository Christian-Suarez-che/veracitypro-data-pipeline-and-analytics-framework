-- models/stg/stg_keepa_sales_stats.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_sales_stats_id',
    incremental_strategy = 'merge',
    tags = ['keepa','sales','stats']
) }}

with base as (
  select
    asin,
    sold_last_30d,
    sold_last_90d,
    rank_drops_30d,
    rank_median_30d,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
  where asin is not null
),

dedup as (
  select
    md5(asin || '|' || to_varchar(ingest_ts)) as stg_sales_stats_id,
    *
  from base
)

select *
from dedup
{% if is_incremental() %}
where ingest_ts > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
{% endif %}
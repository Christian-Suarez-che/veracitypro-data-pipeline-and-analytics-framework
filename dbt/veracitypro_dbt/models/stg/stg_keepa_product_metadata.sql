-- models/stg/stg_keepa_product_metadata.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_product_meta_id',
    incremental_strategy = 'merge',
    tags = ['keepa','product','metadata']
) }}

with base as (
  select
    asin,
    title,
    brand,
    manufacturer,
    binding,
    color,
    product_group,
    product_type_id,
    website_display_group,
    website_display_group_name,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
  where asin is not null
),

dedup as (
  select
    md5(asin || '|' || to_varchar(ingest_ts)) as stg_product_meta_id,
    *
  from base
)

select *
from dedup
{% if is_incremental() %}
where ingest_ts > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
{% endif %}
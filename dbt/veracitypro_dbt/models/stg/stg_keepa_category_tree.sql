-- models/stg/stg_keepa_category_tree.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_category_row_id',
    incremental_strategy = 'merge',
    tags = ['keepa','category']
) }}

with base as (
  select
    asin,
    category_tree_v,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
  where category_tree_v is not null
),

flattened as (
  select
    b.asin,
    f.value:id::number          as category_id,
    f.value:parent::number      as parent_category_id,
    f.value:level::number       as category_level,
    f.value:name::string        as category_name,
    b.ingest_ts
  from base b,
       lateral flatten(input => b.category_tree_v) f
),

dedup as (
  select
    md5(
      asin || '|' ||
      coalesce(to_varchar(category_id),'NULL') || '|' ||
      coalesce(to_varchar(parent_category_id),'NULL')
    ) as stg_category_row_id,
    *
  from flattened
)

select *
from dedup
{% if is_incremental() %}
where ingest_ts > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
{% endif %}
-- models/stg/stg_keepa_category_tree.sql
{{ config(
    materialized='incremental',
    unique_key='stg_category_row_id',
    incremental_strategy='merge',
    tags=['keepa','category_tree']
) }}

with base as (
  select
    asin,
    category_tree_v,
    ingest_ts
  from VP_DWH.STG._stg_keepa_base
  where category_tree_v is not null
  {% if is_incremental() %}
    and ingest_ts > (
      select coalesce(max(ingest_ts), '1900-01-01')
      from {{ this }}
    )
  {% endif %}
),

flattened as (
  select
    b.asin,
    f.value:id::number     as category_id,
    f.value:parent::number as parent_category_id,
    f.value:level::number  as category_level,
    f.value:name::string   as category_name,
    b.ingest_ts
  from base b,
       lateral flatten(input => b.category_tree_v) f
),

dedup as (
  select
    md5(
      coalesce(asin,'NULL') || '|' ||
      coalesce(to_varchar(category_id),'NULL') || '|' ||
      coalesce(to_varchar(parent_category_id),'NULL')
    ) as stg_category_row_id,
    asin,
    category_id,
    parent_category_id,
    category_level,
    category_name,
    ingest_ts,
    row_number() over (
      partition by asin, category_id, parent_category_id
      order by ingest_ts desc
    ) as rn
  from flattened
)

select
  stg_category_row_id,
  asin,
  category_id,
  parent_category_id,
  category_level,
  category_name,
  ingest_ts
from dedup
where rn = 1
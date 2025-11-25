-- models/core/core_keepa_category_hierarchy.sql
{{ config(materialized='table') }}

with ranked as (
  select
    *,
    row_number() over (
      partition by asin, category_id
      order by ingest_ts desc
    ) as rn
  from {{ ref('stg_keepa_category_tree') }}
)

select
  asin,
  category_id,
  parent_category_id,
  category_level,
  category_name
from ranked
where rn = 1
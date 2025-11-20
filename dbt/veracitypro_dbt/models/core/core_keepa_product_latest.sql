-- Summary:
-- - Latest stable product attributes per ASIN to feed dim_product.
{{ config(materialized='incremental', unique_key='asin', incremental_strategy='merge') }}

with pick as (
  select
    asin,
    brand,
    binding,
    color,
    website_display_group,
    website_display_group_name,
    category_tree_v,
    ingest_ts,
    row_number() over (partition by asin order by ingest_ts desc) rn
  from {{ ref('_stg_keepa_base') }}
)

select
  asin,
  brand,
  binding,
  color,
  website_display_group,
  website_display_group_name,
  category_tree_v
from pick
where rn = 1

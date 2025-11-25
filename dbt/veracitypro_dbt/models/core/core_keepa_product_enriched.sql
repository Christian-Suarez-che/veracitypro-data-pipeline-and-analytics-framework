-- models/core/core_keepa_product_enriched.sql
{{ config(materialized='table') }}

with ranked as (
  select
    *,
    row_number() over (
      partition by asin
      order by ingest_ts desc
    ) as rn
  from {{ ref('stg_keepa_product_metadata') }}
)

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
  website_display_group_name
from ranked
where rn = 1
-- models/marts/dim_product_enriched.sql
{{ config(materialized='table') }}

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
from {{ ref('core_keepa_product_enriched') }}
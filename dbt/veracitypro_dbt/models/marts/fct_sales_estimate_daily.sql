-- models/marts/fct_sales_estimate_daily.sql
{{ config(materialized='table') }}

select
  md5(asin || '|SALES_ESTIMATE') as pk,
  asin,
  est_units_per_day_30d,
  est_units_per_day_90d,
  sold_last_30d,
  sold_last_90d
from {{ ref('core_keepa_sales_estimate_daily') }}
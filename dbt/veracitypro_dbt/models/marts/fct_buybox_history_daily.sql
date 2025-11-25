-- models/marts/fct_buybox_history_daily.sql
{{ config(materialized='table') }}

select
  md5(
    asin || '|' ||
    to_varchar(date) || '|' ||
    coalesce(seller_id,'NULL')
  ) as pk,
  asin,
  date,
  seller_id
from {{ ref('core_keepa_buybox_daily') }}
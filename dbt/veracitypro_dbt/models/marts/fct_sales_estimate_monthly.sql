-- models/marts/fct_sales_estimate_monthly.sql
{{ config(materialized='table') }}

select
    md5(
        asin || '|' ||
        to_varchar(sales_month) || '|SALES_ESTIMATE_MONTHLY'
    ) as pk,
    asin,
    sales_month,
    est_units_sold
from {{ ref('core_keepa_sales_estimate_monthly') }}
where est_units_sold is not null
  and est_units_sold >= 0
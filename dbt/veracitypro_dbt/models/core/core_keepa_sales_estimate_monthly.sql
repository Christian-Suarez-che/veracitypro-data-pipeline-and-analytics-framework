-- models/core/core_keepa_sales_estimate_monthly.sql
{{ config(materialized='table') }}

with ranked as (
    select
        asin,
        sales_month,
        est_units_sold,
        ingest_ts,
        row_number() over (
            partition by asin, sales_month
            order by ingest_ts desc
        ) as rn
    from {{ ref('stg_keepa_sales_monthly_stats') }}
),

latest as (
    select
        asin,
        sales_month,
        est_units_sold
    from ranked
    where rn = 1
)

select
    asin,
    sales_month,
    est_units_sold
from latest
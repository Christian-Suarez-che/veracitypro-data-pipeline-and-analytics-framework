-- models/stg/stg_keepa_sales_monthly_stats.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_sales_stats_id',
    incremental_strategy = 'merge',
    tags = ['keepa', 'sales', 'stats']
) }}

with base as (
    select
        asin,
        monthly_sold,
        monthly_sold_history_v,
        ingest_ts
    from {{ ref('_stg_keepa_base') }}
    where asin is not null
),

exploded as (
    select
        asin,
        ingest_ts,
        seq.index::number  as idx,
        seq.value::number  as val
    from base,
    lateral flatten(input => monthly_sold_history_v) as seq
),

paired as (
    select
        asin,
        ingest_ts,
        floor(idx / 2)                              as pair_id,
        max(case when mod(idx, 2) = 0 then val end) as keepa_minutes,
        max(case when mod(idx, 2) = 1 then val end) as est_units_sold
    from exploded
    group by asin, ingest_ts, floor(idx / 2)
),

normalized as (
    select
        asin,
        ingest_ts,
        date_trunc(
            'month',
            dateadd(
                minute,
                keepa_minutes,
                to_timestamp_tz('2011-01-01 00:00:00 +00:00')
            )
        ) as sales_month,
        est_units_sold
    from paired
    where keepa_minutes is not null
),

dedup as (
    select
        md5(asin || '|' || to_varchar(sales_month)) as stg_sales_stats_id,
        asin,
        sales_month,
        est_units_sold,
        ingest_ts
    from normalized
)

select *
from dedup
{% if is_incremental() %}
where ingest_ts > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
{% endif %}
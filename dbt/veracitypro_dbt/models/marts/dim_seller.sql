{{ config(materialized='table', unique_key='seller_id') }}
with latest as (
  select seller_id, row_number() over (partition by seller_id order by ingest_ts desc) rn
  from {{ ref('stg_keepa_offers') }}
  where seller_id is not null
)
select seller_id from latest where rn = 1;

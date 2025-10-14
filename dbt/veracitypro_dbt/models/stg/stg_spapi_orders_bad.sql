{{ config(materialized='incremental',
          unique_key='stg_bad_id',
          incremental_strategy='merge',
          schema='STG_BAD') }}

with base as (select * from {{ ref('_stg_spapi_base') }})
select
  md5(coalesce(amazon_order_id,'NULL')||'|'||coalesce(asin,'NULL')||'|'||coalesce(to_varchar(purchase_ts),'NULL')) as stg_bad_id,
  *
from base
where amazon_order_id is null
   or asin is null
   or purchase_ts is null
   or quantity_ordered < 0
   or item_price < 0

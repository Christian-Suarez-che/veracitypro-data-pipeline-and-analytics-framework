{{ config(materialized='incremental',
          unique_key='stg_bad_id',
          incremental_strategy='merge',
          schema='STG_BAD') }}

with base as (select * from {{ ref('_stg_scraper_base') }})
select
  md5(coalesce(asin,'NULL')||'|'||coalesce(to_varchar(event_ts),'NULL')||'|'||coalesce(to_varchar(price),'NULL')||'|'||coalesce(filename,'NULL')) as stg_bad_id,
  *
from base
where asin is null
   or event_ts is null
   or price is null
   or price < 0

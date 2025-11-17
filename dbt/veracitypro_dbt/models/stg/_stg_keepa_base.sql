-- models/stg/_stg_keepa_base.sql
{{ config(materialized='incremental', unique_key='stg_row_id', incremental_strategy='merge') }}

-- Reads from a STREAM over RAW.KEEPA_RAW that was populated by your COPY INTO
-- Each row is ONE product object under payload:_airbyte_data (NDJSON)
with src as (
  select
    payload,
    filename,               -- from COPY INTO
    last_modified,          -- from COPY INTO
    _airbyte_raw_id,
    _airbyte_extracted_at,
    ingest_dt
  from {{ source('RAW','KEEPA_RAW_STRM') }}
  {% if is_incremental() %}
    where ingest_dt > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
  {% endif %}
),

base as (
  select
    payload:_airbyte_data          as product_v,           -- the Keepa product object
    filename,
    last_modified,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    ingest_dt                      as ingest_ts
  from src
),

final as (
  select
    product_v:asin::string                         as asin,
    product_v:brand::string                        as brand,
    product_v:binding::string                      as binding,
    product_v:color::string                        as color,
    product_v:availabilityAmazon::number           as availability_amazon,
    product_v:websiteDisplayGroup::string          as website_display_group,
    product_v:websiteDisplayGroupName::string      as website_display_group_name,

    -- pass nested structures through for downstream flattening
    product_v:csv                                  as csv_v,
    product_v:salesRanks                           as salesranks_v,
    product_v:offers                               as offers_v,
    product_v:aPlus                                as aplus_v,
    product_v:videos                               as videos_v,
    product_v:categoryTree                         as category_tree_v,

    -- lineage
    filename,
    last_modified,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    ingest_ts
  from base
)

select
  md5(coalesce(asin,'NULL')||'|'||to_varchar(ingest_ts)||'|'||coalesce(_airbyte_raw_id,'NULL')) as stg_row_id,
  *
from final;
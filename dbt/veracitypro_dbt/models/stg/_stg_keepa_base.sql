-- models/stg/_stg_keepa_base.sql
{{ config(materialized='incremental', unique_key='stg_row_id', incremental_strategy='merge') }}

with src as (
  select
    payload,
    null::string        as filename,
    null::timestamp_tz  as last_modified,
    payload:"_airbyte_raw_id"::string              as _airbyte_raw_id,
    payload:"_airbyte_extracted_at"::timestamp_tz  as _airbyte_extracted_at,
    ingest_dt
  from {{ source('RAW','KEEPA_RAW_STRM') }}
  {% if is_incremental() %}
    where ingest_dt > (select coalesce(max(ingest_ts),'1900-01-01') from {{ this }})
  {% endif %}
),

base as (
  select
    payload:_airbyte_data          as product_v,
    filename,
    last_modified,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    ingest_dt                      as ingest_ts
  from src
),

final as (
  select
    -- BASIC PRODUCT METADATA
    product_v:asin::string                         as asin,
    product_v:title::string                        as title,
    product_v:brand::string                        as brand,
    product_v:binding::string                      as binding,
    product_v:color::string                        as color,
    product_v:manufacturer::string                 as manufacturer,
    product_v:productGroup::string                 as product_group,
    product_v:productType::number                  as product_type_id,

    product_v:availabilityAmazon::number           as availability_amazon,
    product_v:websiteDisplayGroup::string          as website_display_group,
    product_v:websiteDisplayGroupName::string      as website_display_group_name,

    -- NESTED STRUCTURES FOR DOWNSTREAM FLATTENING
    product_v:csv                                  as csv_v,
    product_v:salesRanks                           as salesranks_v,
    product_v:offers                               as offers_v,
    product_v:aPlus                                as aplus_v,
    product_v:videos                               as videos_v,
    product_v:categoryTree                         as category_tree_v,

    -- EXTRA TIME-SERIES / STATS
    product_v:buyBoxSellerIdHistory                as buybox_history_v,
    product_v:ratingHistory                        as rating_history_v,
    product_v:reviewCountHistory                   as review_count_history_v,

    -- SALES STATS (adjust JSON paths if needed)
    product_v:stats.soldLast30Days::number         as sold_last_30d,
    product_v:stats.soldLast90Days::number         as sold_last_90d,
    product_v:stats.salesRankDrops30Days::number   as rank_drops_30d,
    product_v:stats.salesRankMedian30Days::number  as rank_median_30d,

    -- lineage
    filename,
    last_modified,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    ingest_ts
  from base
)

select
  md5(
    coalesce(asin,'NULL')
    || '|' || to_varchar(ingest_ts)
    || '|' || coalesce(_airbyte_raw_id,'NULL')
  ) as stg_row_id,
  final.*
from final
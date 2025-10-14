{{ config(
    materialized = 'incremental',
    unique_key   = 'stg_id',
    incremental_strategy = 'merge',
    tags = ['stg','spapi']
) }}

-- 1) Raw source with robust fallbacks and incremental prune
with src as (
  select
    payload,
    coalesce(ingest_ts, ingest_dt)::timestamp_ntz as ingest_ts,
    coalesce(filename, metadata$filename)::string  as filename
  from {{ source('raw','SPAPI_RAW') }}
  {% if is_incremental() %}
    where coalesce(ingest_ts, ingest_dt) >
          (select coalesce(max(ingest_ts),'1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

-- 2) Orders envelope (normalize top-level fields)
orders as (
  select
    payload:"AmazonOrderId"::string                                            as order_id,
    try_to_timestamp((payload:"PurchaseDate")::string)                         as purchase_ts,
    payload:"OrderStatus"::string                                              as order_status,
    payload:"MarketplaceId"::string                                            as marketplace_id,
    payload                                                                    as payload_full,
    s.ingest_ts,
    s.filename
  from src s
),

-- 3) Flatten items under OrderItems (outer => true, then filter)
items as (
  select
    o.order_id,
    /* normalize identifiers */
    upper(regexp_replace(trim(i.value:"ASIN"::string), '\\s+', ''))            as asin,
    regexp_replace(trim(i.value:"SellerSKU"::string), '\\s+', ' ')             as seller_sku,

    /* quantities and pricing */
    try_to_number((i.value:"QuantityOrdered")::string)                         as quantity_ordered,
    try_to_number((i.value:"ItemPrice":"Amount")::string)                      as item_price,
    coalesce(i.value:"ItemPrice":"CurrencyCode"::string, 'USD')                as currency_code,

    /* carry through */
    o.purchase_ts,
    o.order_status,
    o.marketplace_id,
    o.filename,
    o.ingest_ts
  from orders o,
       lateral flatten(input => o.payload_full:"OrderItems", outer => true) i
  where i.value is not null
),

-- 4) Final shape + basic cleaning
base as (
  select
    order_id,
    asin,
    seller_sku,
    /* ensure non-negative */
    case when quantity_ordered is not null and quantity_ordered >= 0 then quantity_ordered end as quantity_ordered,
    case when item_price       is not null and item_price       >= 0 then item_price       end as item_price,
    currency_code,
    purchase_ts,
    order_status,
    marketplace_id,
    filename,
    ingest_ts,
    to_date(ingest_ts) as snapshot_date
  from items
),

-- 5) Validate rows we keep
valid as (
  select *
  from base
  where order_id is not null
    and asin     is not null
    and purchase_ts is not null
    and coalesce(quantity_ordered,0) >= 0
    and coalesce(item_price,0)       >= 0
),

-- SP-API: Orders + OrderItems flattened to one row per (order_id, asin)
with src as (
  select payload, ingest_dt
  from {{ source('raw','SPAPI_RAW') }}
),
orders as (
  select
    payload:"AmazonOrderId"::string                     as order_id,
    try_to_timestamp((payload:"PurchaseDate")::string)  as purchase_ts,
    payload:"OrderStatus"::string                       as order_status,
    payload:"MarketplaceId"::string                     as marketplace_id,
    payload                                             as payload_full,
    ingest_dt
  from src
),
items as (
  select
    o.order_id,
    i.value:"ASIN"::string                                 as asin,
    try_to_number((i.value:"QuantityOrdered")::string)     as qty,
    try_to_number((i.value:"ItemPrice":"Amount")::string)  as item_price,
    o.purchase_ts,
    o.order_status,
    o.marketplace_id,
    o.ingest_dt
  from orders o,
       lateral flatten(input => o.payload_full:"OrderItems", outer => true) i
  where i.value is not null   -- keep only rows where we actually have an item
)
select * from items

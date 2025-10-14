{{ config(materialized='view') }}

{% set bounds = vp_prev_day_bounds(var('as_of_date', current_date()|string)) %}

with raw as (
  select
    payload,
    ingest_ts,
    metadata$filename::string as filename
  from RAW.SPAPI_ORDERS_RAW_STRM
  where ingest_ts between {{ bounds['start'] }} and {{ bounds['end'] }}
),
norm as (
  select
    upper(trim(payload:'ASIN'::string))                                  as asin,
    trim(payload:'AmazonOrderId'::string)                                 as amazon_order_id,
    try_to_timestamp_ntz(payload:'PurchaseDate'::string)                  as purchase_ts,
    try_to_number(payload:'QuantityOrdered'::string)                      as quantity_ordered,
    try_to_number(payload:'ItemPrice'::string)                            as item_price,
    try_to_number(payload:'ItemTax'::string)                              as item_tax,
    filename,
    ingest_ts,
    to_date({{ bounds['start'] }})                                        as snapshot_date
  from raw
)
select * from norm

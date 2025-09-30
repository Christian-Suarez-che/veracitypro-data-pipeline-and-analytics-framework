-- ScraperAPI: offers snapshot (array -> rows)
with src as (
  select payload
  from {{ source('raw','SCRAPERAPI_RAW') }}
),
offers as (
  select
    src.payload:"asin"::string                          as asin,
    try_to_timestamp((src.payload:"scraped_at")::string) as snapshot_ts,
    o.value:"sold_by"::string                           as sold_by,
    o.value:"ships_from"::string                        as ships_from,
    try_to_number((o.value:"price")::string)            as price,
    try_to_number(coalesce((o.value:"shipping_price")::string, '0')) as shipping_price,
    coalesce(o.value:"has_prime"::boolean, false)       as has_prime_shipping,
    coalesce(o.value:"is_fba"::boolean, false)          as is_fba
  from src,
  lateral flatten(input => src.payload:"offers") o
)
select * from offers

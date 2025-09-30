-- ScraperAPI: product page snapshot
with src as (
  select payload, ingest_dt
  from {{ source('raw','SCRAPERAPI_RAW') }}
)
select
  payload:"asin"::string                  as asin,
  payload:"title"::string                 as title,
  payload:"brand"::string                 as brand,
  payload:"category"::string              as category,
  try_to_number((payload:"price")::string) as scraped_price,
  try_to_timestamp((payload:"scraped_at")::string) as scraped_at,
  ingest_dt
from src

-- Keepa: price timeline (priceEvents)
-- Robust timestamp handling + safe numeric casts
with src as (
  select
    payload,
    ingest_dt,
    payload:"asin"::string as asin
  from {{ source('raw','KEEPA_RAW') }}
),
events as (
  select
    s.asin,

    /* event_ts can be ISO string or epoch (s/ms) */
    case
      when try_to_timestamp((e.value:"ts")::string) is not null
        then try_to_timestamp((e.value:"ts")::string)
      when try_to_number((e.value:"ts")::string) >= 100000000000    -- >= 1e11 → likely ms
        then to_timestamp_ntz( try_to_number((e.value:"ts")::string) / 1000 )
      when try_to_number((e.value:"ts")::string) is not null
        then to_timestamp_ntz( try_to_number((e.value:"ts")::string) )
      else null
    end as event_ts,

    try_to_number((e.value:"price")::string) as price_cents,
    s.ingest_dt
  from src s,
  lateral flatten(input => s.payload:"priceEvents") e
)
select
  asin,
  event_ts,
  case when price_cents is not null then price_cents / 100.0 end as price_usd,
  ingest_dt
from events
where event_ts is not null

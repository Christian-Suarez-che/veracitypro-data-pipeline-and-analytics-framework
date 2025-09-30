-- Keepa: sales rank timeline (rankEvents)
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

    case
      when try_to_timestamp((e.value:"ts")::string) is not null
        then try_to_timestamp((e.value:"ts")::string)
      when try_to_number((e.value:"ts")::string) >= 100000000000
        then to_timestamp_ntz( try_to_number((e.value:"ts")::string) / 1000 )
      when try_to_number((e.value:"ts")::string) is not null
        then to_timestamp_ntz( try_to_number((e.value:"ts")::string) )
      else null
    end as event_ts,

    try_to_number((e.value:"rank")::string) as sales_rank,
    s.ingest_dt
  from src s,
  lateral flatten(input => s.payload:"rankEvents") e
)
select asin, event_ts, sales_rank, ingest_dt
from events
where event_ts is not null

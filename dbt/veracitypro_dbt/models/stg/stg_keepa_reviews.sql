with base as (select asin, csv_v, ingest_ts from {{ ref('_stg_keepa_base') }}),

r as (
  select asin, seq4() as pos,
         (csv_v[16][2*pos])::number   as min_r, (csv_v[16][2*pos+1])::number as raw_rating
  from base
  qualify 2*pos+1 < array_size(csv_v[16])
),
c as (
  select asin, seq4() as pos,
         (csv_v[17][2*pos])::number   as min_c, (csv_v[17][2*pos+1])::number as raw_count
  from base
  qualify 2*pos+1 < array_size(csv_v[17])
),
joined as (
  select
    coalesce(r.asin, c.asin) asin,
    coalesce(r.min_r, c.min_c) as keepa_min,
    r.raw_rating, c.raw_count
  from r
  full outer join c
    on r.asin = c.asin and r.min_r = c.min_c
)
select
  md5(asin||'|'||to_varchar(to_timestamp_ntz((keepa_min+21564000)*60))) as pk,
  asin,
  to_timestamp_ntz((keepa_min+21564000)*60) as snapshot_ts,
  iff(raw_rating < 0, null, raw_rating/10.0) as rating,           -- Keepa: 45 => 4.5
  iff(raw_count < 0, null, raw_count)        as review_count
from joined;
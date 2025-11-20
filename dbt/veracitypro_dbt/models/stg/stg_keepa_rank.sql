-- models/stg/stg_keepa_rank.sql

with base as (
  select
    asin,
    csv_v,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
),
pairs as (
  -- Flatten the rank channel (index 3) and regroup every 2 elements into (minute, rank) pairs
  select
    b.asin,
    floor(f.index / 2)::number as pair_idx,  -- each pair_idx = one (minute, rank) time point
    max(iff(mod(f.index, 2) = 0, f.value::number, null)) as keepa_min,
    max(iff(mod(f.index, 2) = 1, f.value::number, null)) as raw_rank,
    b.ingest_ts
  from base b,
       lateral flatten(input => b.csv_v[3]) f
  group by
    b.asin,
    b.ingest_ts,
    floor(f.index / 2)
)
select
  md5(asin||'|'||to_varchar(to_timestamp_ntz((keepa_min+21564000)*60))) as pk,
  asin,
  to_timestamp_ntz((keepa_min+21564000)*60) as snapshot_ts,
  iff(raw_rank < 0, null, raw_rank) as rank,
  ingest_ts
from pairs
{% if is_incremental() %}
where snapshot_ts::date >= (
  select coalesce(max(snapshot_ts::date),'1900-01-01') from {{ this }}
)
{% endif %}

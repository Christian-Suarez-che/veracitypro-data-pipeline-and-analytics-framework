with base as (select asin, csv_v, ingest_ts from {{ ref('_stg_keepa_base') }}),
pairs as (
  select asin,
         seq4() as pos,
         (csv_v[3][2*pos])::number  as keepa_min,
         (csv_v[3][2*pos+1])::number as raw_rank,
         ingest_ts
  from base
  qualify 2*pos+1 < array_size(csv_v[3])
)
select
  md5(asin||'|'||to_varchar(to_timestamp_ntz((keepa_min+21564000)*60))) as pk,
  asin,
  to_timestamp_ntz((keepa_min+21564000)*60) as snapshot_ts,
  iff(raw_rank < 0, null, raw_rank) as rank,
  ingest_ts
from pairs
{% if is_incremental() %} where snapshot_ts::date >= (select coalesce(max(snapshot_ts::date),'1900-01-01') from {{ this }}) {% endif %};
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='bad_row_id', schema='STG_BAD', tags=['quarantine','keepa','reviews']) }}
with good as (select asin, snapshot_ts, rating, review_count, ingest_ts from {{ ref('stg_keepa_reviews') }}),
bad as (
  select
    md5(coalesce(asin,'NULL')||'|'||coalesce(to_varchar(snapshot_ts),'NULL')||'|'||coalesce(to_varchar(ingest_ts),'NULL')) as bad_row_id,
    asin, snapshot_ts, rating, review_count, ingest_ts,
    case
      when asin is null then 'ASIN_NULL'
      when snapshot_ts is null then 'TS_NULL'
      when rating is not null and (rating < 0 or rating > 5) then 'RATING_OOB'
      when review_count is not null and review_count < 0 then 'COUNT_NEGATIVE'
      else 'UNKNOWN'
    end as reason_code,
    'Reviews staging validation failure' as reason_detail
  from good
  where asin is null or snapshot_ts is null or
        (rating is not null and (rating < 0 or rating > 5)) or
        (review_count is not null and review_count < 0)
)
select * from bad;

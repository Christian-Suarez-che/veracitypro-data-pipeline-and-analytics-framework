{{ config(materialized='incremental', incremental_strategy='merge', unique_key='bad_row_id', schema='STG_BAD', tags=['quarantine','keepa','offers']) }}
with src as (select asin, offer_id, seller_id, offer_csv_raw, ingest_ts from {{ ref('stg_keepa_offers') }}),
bad as (
  select
    md5(coalesce(asin,'NULL')||'|'||coalesce(offer_id,'NULL')||'|'||coalesce(to_varchar(ingest_ts),'NULL')) as bad_row_id,
    asin, offer_id, seller_id, offer_csv_raw, ingest_ts,
    case
      when asin is null then 'ASIN_NULL'
      when offer_id is null then 'OFFER_ID_NULL'
      when offer_csv_raw is null or offer_csv_raw = '' then 'OFFER_CSV_EMPTY'
      else 'UNKNOWN'
    end as reason_code,
    'Offers staging validation failure' as reason_detail
  from src
  where asin is null or offer_id is null or offer_csv_raw is null or offer_csv_raw = ''
)
select * from bad;

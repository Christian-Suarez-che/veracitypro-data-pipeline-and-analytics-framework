-- Roll-up view for quick monitoring of all quarantines.
{{ config(materialized='view') }}
select 'price'   as table_name, bad_row_id, asin, snapshot_ts, reason_code, reason_detail, ingest_ts
from {{ ref('stg_keepa_price__bad') }}
union all
select 'rank',    bad_row_id, asin, snapshot_ts, reason_code, reason_detail, ingest_ts
from {{ ref('stg_keepa_rank__bad') }}
union all
select 'reviews', bad_row_id, asin, snapshot_ts, reason_code, reason_detail, ingest_ts
from {{ ref('stg_keepa_reviews__bad') }}
union all
select 'offers',  bad_row_id, asin, null,       reason_code, reason_detail, ingest_ts
from {{ ref('stg_keepa_offers__bad') }}
union all
select 'aplus',   bad_row_id, asin, null,       reason_code, reason_detail, ingest_ts
from {{ ref('stg_keepa_aplus_module__bad') }};

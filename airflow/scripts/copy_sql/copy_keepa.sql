-- COPY Keepa raw JSON from S3 (via RAW.STAGE_KEEPA) into RAW.KEEPA_RAW
-- Tolerant to either /YYYY-MM-DD/ or /date=YYYY-MM-DD/ layouts.
use role AIRFLOW_ROLE;
use database VP_DWH;
use schema RAW;

-- Choose your JSON format:
--   RAW.FF_JSON_NESTED  -> files are one big outer array
--   RAW.FF_JSON         -> files are NDJSON (one JSON object per line)
set ff_name = 'RAW.FF_JSON_NESTED';   -- <-- change to RAW.FF_JSON if NDJSON

-- Airflow supplies {{ ds }} as YYYY-MM-DD. We match both styles:
--   …/YYYY-MM-DD/…           OR    …/date=YYYY-MM-DD/…
copy into RAW.KEEPA_RAW (payload, filename, last_modified)
from (
  select
    $1,
    metadata$filename::string,
    metadata$last_modified
  from @RAW.STAGE_KEEPA
)
file_format = (format_name => identifier($ff_name))
pattern     = '.*(/|^)({{ ds }}|date={{ ds }})(/|/.*\\.json(\\.gz|\\.zst)?)$'
on_error    = 'skip_file'
-- force   = true                 -- only if you intentionally re-load same filenames
-- validation_mode = return_errors -- dry-run without loading
;

-- COPY ScraperAPI raw JSON from S3 (via RAW.STAGE_SCRAPERAPI) into RAW.SCRAPERAPI_RAW
-- Tolerant to either /YYYY-MM-DD/ or /date=YYYY-MM-DD/ layouts.

use role AIRFLOW_ROLE;
use database VP_DWH;
use schema RAW;

-- Pick correct JSON format for ScraperAPI output:
--   RAW.FF_JSON        -> NDJSON (most common for scrapers)
--   RAW.FF_JSON_NESTED -> single outer array
set ff_name = 'RAW.FF_JSON';  -- switch to RAW.FF_JSON_NESTED if you export arrays

copy into RAW.SCRAPERAPI_RAW (payload, filename, last_modified)
from (
  select
    $1,
    metadata$filename::string,
    metadata$last_modified
  from @RAW.STAGE_SCRAPERAPI
)
file_format = (format_name => identifier($ff_name))
pattern     = '.*(/|^)({{ ds }}|date={{ ds }})(/|/.*\\.json(\\.gz|\\.zst)?)$'
on_error    = 'skip_file'
-- force   = true
-- validation_mode = return_errors
;
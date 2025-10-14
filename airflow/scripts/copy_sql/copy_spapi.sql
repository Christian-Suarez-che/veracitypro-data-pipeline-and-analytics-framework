-- COPY Amazon SP-API raw JSON from S3 (via RAW.STAGE_SPAPI) into RAW.SPAPI_RAW
-- Tolerant to either /YYYY-MM-DD/ or /date=YYYY-MM-DD/ layouts.

use role AIRFLOW_ROLE;
use database VP_DWH;
use schema RAW;

-- SP-API exports vary: choose NDJSON vs outer array accordingly
set ff_name = 'RAW.FF_JSON';  -- switch to RAW.FF_JSON_NESTED if your dumps are arrays

copy into RAW.SPAPI_RAW (payload, filename, last_modified)
from (
  select
    $1,
    metadata$filename::string,
    metadata$last_modified
  from @RAW.STAGE_SPAPI
)
file_format = (format_name => identifier($ff_name))
pattern     = '.*(/|^)({{ ds }}|date={{ ds }})(/|/.*\\.json(\\.gz|\\.zst)?)$'
on_error    = 'skip_file'
-- force   = true
-- validation_mode = return_errors
;
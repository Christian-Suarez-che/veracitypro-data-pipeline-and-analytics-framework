/* 03_stages_file_formats.sql  (DEV)
   - Creates S3 storage integration (bucket-wide)
   - Creates file formats
   - Creates external stages in RAW / CORE / MONITORING
   - Minimal RAW landing tables
*/

-------------------------------
-- 3.1 STORAGE INTEGRATION
--   (ACCOUNTADMIN; account-level object)
-------------------------------
use role ACCOUNTADMIN;
use database VP_DWH;

create or replace storage integration VP_S3_INT
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::471451201197:role/vp-snowflake-s3-role'
  -- allow entire buckets so any folder inside is accessible
  storage_allowed_locations = (
    's3://vp-raw-dev-us-east-2/',
    's3://vp-curated-dev-us-east-2/',
    's3://vp-monitoring-dev-us-east-2/'
  )
  comment = 'Secure S3 access for RAW / CURATED / MONITORING (dev)';
;

-- let SYSADMIN create/own stages that use this integration
grant usage on integration VP_S3_INT to role SYSADMIN;

-- (run this once to copy values for AWS trust)
-- desc integration VP_S3_INT;

-------------------------------
-- 3.2 FILE FORMATS + RAW STAGES
--   (SYSADMIN; database-level objects)
-------------------------------

use database VP_DWH;
use schema RAW;

-- File formats (JSON)
create or replace file format FF_JSON
  type = json
  strip_outer_array = true;

create or replace file format FF_JSON_NESTED
  type = json;

-- RAW external stages (read sources)
create or replace stage STAGE_KEEPA
  url='s3://vp-raw-dev-us-east-2/env=dev/source=keepa/'
  storage_integration=VP_S3_INT
  file_format=FF_JSON_NESTED
  comment='All Keepa JSON (nested), any subfolders';

create or replace stage STAGE_SCRAPERAPI
  url='s3://vp-raw-dev-us-east-2/env=dev/source=scraperapi/'
  storage_integration=VP_S3_INT
  file_format=FF_JSON
  comment='All ScraperAPI JSON (product/offers/other)';

create or replace stage STAGE_SPAPI
  url='s3://vp-raw-dev-us-east-2/env=dev/source=spapi/'
  storage_integration=VP_S3_INT
  file_format=FF_JSON
  comment='All Amazon SP-API JSON (orders/listings/etc.)';

-------------------------------
-- 3.3 MINIMAL RAW LANDING TABLES
-------------------------------
create or replace table KEEPA_RAW      (payload variant, ingest_dt date default current_date());
create or replace table SCRAPERAPI_RAW (payload variant, ingest_dt date default current_date());
create or replace table SPAPI_RAW      (payload variant, ingest_dt date default current_date());

-------------------------------
-- 3.4 OPTIONAL: CURATED STAGES (write targets)
--    Put these in the schema where you'll UNLOAD curated data.
-------------------------------
use schema CORE;

create or replace stage STAGE_CURATED_COMPETITORS
  url='s3://vp-curated-dev-us-east-2/domain=competitors/'
  storage_integration=VP_S3_INT
  comment='Curated exports: competitors domain';

create or replace stage STAGE_CURATED_PRICING
  url='s3://vp-curated-dev-us-east-2/domain=pricing/'
  storage_integration=VP_S3_INT
  comment='Curated exports: pricing domain';

-------------------------------
-- 3.5 OPTIONAL: MONITORING STAGES (pipeline logs/metrics)
-------------------------------
use schema MONITORING;

create or replace stage STAGE_MONITORING_AIRBYTE
  url='s3://vp-monitoring-dev-us-east-2/airbyte/'
  storage_integration=VP_S3_INT
  comment='Airbyte run logs / artifacts';

create or replace stage STAGE_MONITORING_AIRFLOW
  url='s3://vp-monitoring-dev-us-east-2/airflow/'
  storage_integration=VP_S3_INT
  comment='Airflow run logs / artifacts';

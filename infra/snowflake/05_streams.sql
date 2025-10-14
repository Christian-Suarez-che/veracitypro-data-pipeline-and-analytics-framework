-- Context: create append-only Streams on RAW landing tables
-- Purpose: let downstream (dbt) read *newly ingested* rows only

use role ACCOUNTADMIN;             -- create/own objects under SYSADMIN (owner = SYSADMIN)
use database VP_DWH;           -- your warehouse database
use schema RAW;                -- RAW landing schema (source tables live here)

-- KEEPA raw CDC (append-only). Tracks only newly inserted files/rows.
create or replace stream KEEPA_RAW_STRM
  on table KEEPA_RAW
  append_only = true
  comment = 'Stream on RAW.KEEPA_RAW; tracks new rows only (append-only).';

-- SCRAPERAPI raw CDC (append-only). Tracks new rows only.
create or replace stream SCRAPERAPI_RAW_STRM
  on table SCRAPERAPI_RAW
  append_only = true
  comment = 'Stream on RAW.SCRAPERAPI_RAW; tracks new rows only (append-only).';

-- SP-API raw CDC (append-only). Tracks new rows only.
create or replace stream SPAPI_RAW_STRM
  on table SPAPI_RAW
  append_only = true
  comment = 'Stream on RAW.SPAPI_RAW; tracks new rows only (append-only).';


-- Privileges for dbt:
--   dbt needs: USAGE on database & schema + SELECT on the stream.
--   (SELECT on the *underlying* table is not required to read the stream.)
-- grant usage on database VP_DWH to role DBT_ROLE;
-- grant usage on schema RAW to role DBT_ROLE;

grant select on stream KEEPA_RAW_STRM      to role DBT_ROLE;
grant select on stream SCRAPERAPI_RAW_STRM to role DBT_ROLE;
grant select on stream SPAPI_RAW_STRM      to role DBT_ROLE;
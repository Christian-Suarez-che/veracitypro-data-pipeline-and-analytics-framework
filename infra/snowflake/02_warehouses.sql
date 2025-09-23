use role ACCOUNTADMIN;

create warehouse if not exists WH_INGEST
  warehouse_size = XSMALL
  auto_suspend   = 300
  auto_resume    = true
  initially_suspended = true
  comment='Ingestion/ELT (Airbyte, COPY, dbt builds)';

create warehouse if not exists WH_BI
  warehouse_size = SMALL
  auto_suspend   = 300
  auto_resume    = true
  initially_suspended = true
  comment='Analytics/BI (Power BI/Tableau/ad-hoc)';

use role ACCOUNTADMIN;

-- Warehouses
grant usage on warehouse WH_INGEST to role AIRBYTE_ROLE;
grant usage on warehouse WH_INGEST to role DBT_ROLE;
grant usage on warehouse WH_INGEST to role AIRFLOW_ROLE;
grant usage on warehouse WH_BI      to role ANALYST_ROLE;
grant usage on warehouse WH_BI      to role DS;

-- Database usage
grant usage on database VP_DWH to role AIRBYTE_ROLE;
grant usage on database VP_DWH to role DBT_ROLE;
grant usage on database VP_DWH to role ANALYST_ROLE;
grant usage on database VP_DWH to role AIRFLOW_ROLE;
grant usage on database VP_DWH to role DS;

-- Schema usage
grant usage on schema VP_DWH.RAW          to role AIRBYTE_ROLE;
grant usage on schema VP_DWH.RAW          to role DBT_ROLE;
grant usage on schema VP_DWH.STG          to role DBT_ROLE;
grant usage on schema VP_DWH.CORE         to role DBT_ROLE;
grant usage on schema VP_DWH.MART         to role ANALYST_ROLE;
grant usage on schema VP_DWH.MART         to role DBT_ROLE;
grant usage on schema VP_DWH.ML           to role DS;
grant usage on schema VP_DWH.MONITORING   to role AIRFLOW_ROLE;

-- RAW: Airbyte writes; dbt reads
grant select, insert on all tables    in schema VP_DWH.RAW to role AIRBYTE_ROLE;
grant select         on all tables    in schema VP_DWH.RAW to role DBT_ROLE;
grant select, insert on future tables in schema VP_DWH.RAW to role AIRBYTE_ROLE;
grant select         on future tables in schema VP_DWH.RAW to role DBT_ROLE;
-- If Airbyte ever loads directly into Snowflake, also grant:
-- grant create table, create stage, create file format on schema VP_DWH.RAW to role AIRBYTE_ROLE;

-- STG/CORE: dbt owns writes
grant select, insert, update, delete on all tables    in schema VP_DWH.STG  to role DBT_ROLE;
grant select, insert, update, delete on all tables    in schema VP_DWH.CORE to role DBT_ROLE;
grant select, insert, update, delete on future tables in schema VP_DWH.STG  to role DBT_ROLE;
grant select, insert, update, delete on future tables in schema VP_DWH.CORE to role DBT_ROLE;
-- dbt must be able to create its models
grant create table, create view on schema VP_DWH.STG  to role DBT_ROLE;
grant create table, create view on schema VP_DWH.CORE to role DBT_ROLE;
grant create table, create view on schema VP_DWH.MART to role DBT_ROLE;
-- ensure dbt can read views it (or others) create
grant select on future views in schema VP_DWH.STG  to role DBT_ROLE;
grant select on future views in schema VP_DWH.CORE to role DBT_ROLE;
grant select on future views in schema VP_DWH.MART to role DBT_ROLE;

-- MART: analysts read-only
grant select on all tables    in schema VP_DWH.MART to role ANALYST_ROLE;
grant select on future tables in schema VP_DWH.MART to role ANALYST_ROLE;
grant select on all views     in schema VP_DWH.MART to role ANALYST_ROLE;
grant select on future views  in schema VP_DWH.MART to role ANALYST_ROLE;

-- ML, MONITORING (optional)
grant select, insert, update, delete on all tables    in schema VP_DWH.ML to role DS;
grant select, insert, update, delete on future tables in schema VP_DWH.ML to role DS;

grant select, insert on all tables    in schema VP_DWH.MONITORING to role AIRFLOW_ROLE;
grant select, insert on future tables in schema VP_DWH.MONITORING to role AIRFLOW_ROLE;
-- (optional) if Airflow should create its own log tables:
-- grant create table on schema VP_DWH.MONITORING to role AIRFLOW_ROLE;

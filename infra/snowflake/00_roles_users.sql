use role ACCOUNTADMIN;

-- Project roles (least privilege)
create role if not exists AIRBYTE_ROLE  comment='Loads to RAW only';
create role if not exists DBT_ROLE      comment='Transforms RAW→STG→CORE→MART';
create role if not exists ANALYST_ROLE  comment='Read-only MART';
create role if not exists AIRFLOW_ROLE  comment='Orchestrator / monitoring';
create role if not exists DS            comment='Data Science (read CORE/MART, write ML)';

-- Visibility under SYSADMIN (admin convenience)
grant role AIRBYTE_ROLE to role SYSADMIN;
grant role DBT_ROLE     to role SYSADMIN;
grant role ANALYST_ROLE to role SYSADMIN;
grant role AIRFLOW_ROLE to role SYSADMIN;
grant role DS           to role SYSADMIN;
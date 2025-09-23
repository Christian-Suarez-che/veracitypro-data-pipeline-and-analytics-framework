use role ACCOUNTADMIN;

create database if not exists VP_DWH comment='VeracityPro Data Warehouse';
use database VP_DWH;

create schema if not exists RAW         comment='Raw ingested, append-only';
create schema if not exists STG         comment='Cleaned/typed staging (dbt)';
create schema if not exists CORE        comment='Conformed business entities';
create schema if not exists MART        comment='Curated marts for BI';
create schema if not exists ML          comment='ML features/outputs';
create schema if not exists MONITORING  comment='Pipeline metadata & run logs';

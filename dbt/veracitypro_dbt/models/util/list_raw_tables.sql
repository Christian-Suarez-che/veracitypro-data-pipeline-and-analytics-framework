select table_catalog, table_schema, table_name
from VP_DWH.INFORMATION_SCHEMA.TABLES
where table_schema = 'RAW'
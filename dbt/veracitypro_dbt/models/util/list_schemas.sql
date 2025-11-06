select catalog_name, schema_name
from information_schema.schemata
where catalog_name = 'VP_DWH'
  and schema_name in ('RAW','STG','CORE','MART','ML','MONITORING')
{{ config(materialized='table', unique_key='pk') }}
select md5(asin||'|'||category_id) as pk, asin, category_id
from {{ ref('core_keepa_product_categories') }};

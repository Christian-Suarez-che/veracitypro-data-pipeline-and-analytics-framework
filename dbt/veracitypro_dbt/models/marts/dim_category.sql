{{ config(materialized='table', unique_key='category_id') }}
select distinct category_id, category_name, parent_category_id
from {{ ref('core_keepa_product_categories') }};
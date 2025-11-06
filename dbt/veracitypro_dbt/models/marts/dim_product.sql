{{ config(materialized='table', unique_key='asin') }}
select asin, brand, binding, color, website_display_group, website_display_group_name
from {{ ref('core_keepa_product_latest') }};
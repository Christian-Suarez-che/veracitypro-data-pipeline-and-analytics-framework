{{ config(materialized='table') }}

select
    md5(asin || '|' || category_id) as pk,
    asin,
    category_id
from {{ ref('core_keepa_product_categories') }}
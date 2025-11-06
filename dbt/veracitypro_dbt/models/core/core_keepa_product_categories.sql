-- Summary:
-- - Flattens category_tree_v to (asin, category_id, category_name, parent_category_id).
{{ config(materialized='table') }}

with latest as (select * from {{ ref('core_keepa_product_latest') }}),
tree as (
  select
    asin,
    t.value:catId::number as category_id,
    t.value:name::string  as category_name,
    lag(t.value:catId::number) over (partition by asin order by t.index) as parent_category_id
  from latest, lateral flatten(input => category_tree_v, outer => true) t
)
select * from tree;
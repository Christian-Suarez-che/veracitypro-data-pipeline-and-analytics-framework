-- models/stg/stg_keepa_aplus_module.sql
-- Summary:
-- - Unnests aPlus[].module[] into atomic assets (image, image_alt, text, video).
-- - Useful for content coverage/quality analyses and A/B testing.
{{ config(materialized='incremental', unique_key='stg_aplus_id', incremental_strategy='merge', tags=['keepa','aplus']) }}

with base as (
  select
    aplus_v,          -- aPlus array from _stg_keepa_base
    asin,
    ingest_ts
  from {{ ref('_stg_keepa_base') }}
),

aplus as (
  select
    b.asin,
    a.index::int     as aplus_idx,
    m.index::int     as module_idx,
    m.value          as mod_v,
    b.ingest_ts
  from base b,
       lateral flatten(input => b.aplus_v, outer => true) a,   -- flatten aPlus array
       lateral flatten(input => a.value:module,  outer => true) m
),
assets as (
  -- one row per asset in the module
  select asin, aplus_idx, module_idx, 'image' as asset_type, i.index::int as asset_idx,
         i.value::string as url, null as alt_text, null as text, ingest_ts
  from aplus, lateral flatten(input => mod_v:image, outer => true) i
  union all
  select asin, aplus_idx, module_idx, 'image_alt', ia.index::int, null, ia.value::string, null, ingest_ts
  from aplus, lateral flatten(input => mod_v:imageAltText, outer => true) ia
  union all
  select asin, aplus_idx, module_idx, 'text', t.index::int, null, null, t.value::string, ingest_ts
  from aplus, lateral flatten(input => mod_v:text, outer => true) t
  union all
  select asin, aplus_idx, module_idx, 'video', v.index::int, v.value::string, null, null, ingest_ts
  from aplus, lateral flatten(input => mod_v:video, outer => true) v
),
dedup as (
  select
    md5(asin||'|'||aplus_idx||'|'||module_idx||'|'||asset_type||'|'||asset_idx) as stg_aplus_id,
    *
  from (
    select a.*, row_number() over (partition by asin, aplus_idx, module_idx, asset_type, asset_idx order by ingest_ts desc) rn
    from assets a
  ) where rn = 1
)
select * from dedup

-- models/stg/stg_keepa_reviews.sql

{{ config(
    materialized = 'incremental',
    unique_key   = 'pk'
) }}

with base as (
    select
        asin,
        csv_v,
        ingest_ts
    from {{ ref('_stg_keepa_base') }}
),

-- Rating history (csv index 16): [keepa_min, rating_int, keepa_min, rating_int, ...]
rating_pairs as (
    select
        b.asin,
        floor(f.index / 2)::number as pair_idx,  -- each pair_idx is one (minute, rating) point
        max(iff(mod(f.index, 2) = 0, f.value::number, null)) as keepa_min_rating,
        max(iff(mod(f.index, 2) = 1, f.value::number, null)) as raw_rating,
        b.ingest_ts
    from base b,
         lateral flatten(input => b.csv_v[16]) f
    group by
        b.asin,
        b.ingest_ts,
        floor(f.index / 2)
),

-- Review count history (csv index 17): [keepa_min, count, keepa_min, count, ...]
count_pairs as (
    select
        b.asin,
        floor(f.index / 2)::number as pair_idx,
        max(iff(mod(f.index, 2) = 0, f.value::number, null)) as keepa_min_count,
        max(iff(mod(f.index, 2) = 1, f.value::number, null)) as raw_count,
        b.ingest_ts
    from base b,
         lateral flatten(input => b.csv_v[17]) f
    group by
        b.asin,
        b.ingest_ts,
        floor(f.index / 2)
),

-- Join rating + count on asin + keepa_min (Keepa time minutes)
joined as (
    select
        coalesce(r.asin, c.asin)                         as asin,
        coalesce(r.keepa_min_rating, c.keepa_min_count) as keepa_min,
        coalesce(r.ingest_ts, c.ingest_ts)              as ingest_ts,
        r.raw_rating,
        c.raw_count
    from rating_pairs r
    full outer join count_pairs c
        on  r.asin             = c.asin
        and r.keepa_min_rating = c.keepa_min_count
)

select
    md5(asin || '|' || to_varchar(to_timestamp_ntz((keepa_min + 21564000) * 60))) as pk,
    asin,
    to_timestamp_ntz((keepa_min + 21564000) * 60)                                  as snapshot_ts,
    -- Keepa rating: 0–50 (e.g. 45 = 4.5 stars). Negative => sentinel (no value).
    iff(raw_rating < 0, null, raw_rating / 10.0)                                   as rating,
    -- Keepa review count: negative => sentinel (no value).
    iff(raw_count  < 0, null, raw_count)                                           as review_count,
    ingest_ts
from joined
{% if is_incremental() %}
where snapshot_ts::date >= (
    select coalesce(max(snapshot_ts::date), '1900-01-01')
    from {{ this }}
)
{% endif %}

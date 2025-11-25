{{ config(materialized='table') }}

with src as (
    select
        asin,
        offer_id,
        date,
        price
    from {{ ref('core_keepa_offer_price_daily') }}
    where date is not null           -- drop any rows with NULL date
)

select
    -- Surrogate primary key: must never be NULL
    md5(
        coalesce(asin, '_UNKNOWN_ASIN_') || '|' ||
        coalesce(to_varchar(offer_id), '_UNKNOWN_OFFER_') || '|' ||
        to_varchar(date)  -- date is guaranteed NOT NULL now
    ) as pk,
    asin,
    offer_id,
    date,
    price
from src
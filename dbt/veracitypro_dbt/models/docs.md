{% docs keepa_price_events %}
**Keepa price events** extracted from `RAW.KEEPA_RAW.payload:priceEvents`.  
`event_ts` can arrive as ISO strings or epoch seconds/milliseconds; we normalize to `TIMESTAMP_NTZ`. Prices are delivered by Keepa in **cents**; we convert to `price_usd`.
Use for price trend analyses and buy box monitoring.
{% enddocs %}

{% docs keepa_rank_events %}
**Keepa rank events** from `RAW.KEEPA_RAW.payload:rankEvents`.  
Each row captures the category **sales rank** at a timestamp. Lower rank means higher sales velocity. Useful for competitiveness and seasonality.
{% enddocs %}

{% docs keepa_rating_events %}
**Keepa rating events** from `RAW.KEEPA_RAW.payload:ratingEvents`.  
Tracks `rating` (0–5) and cumulative `ratings_count` over time for an ASIN.  
Use for social proof and review-velocity analysis.
{% enddocs %}

{% docs scraper_product %}
**ScraperAPI product page snapshot** for an ASIN (`title`, `brand`, `category`, `scraped_price`, `scraped_at`).  
One row per scrape; join to Keepa timelines by ASIN + time windows if needed.
{% enddocs %}

{% docs scraper_offers %}
**ScraperAPI offers snapshot** (merchant-level).  
One row per `(asin, snapshot_ts, sold_by)` including `price`, `shipping_price`, Prime/FBA flags.  
Use for 3P competition tracking and price undercut detection.
{% enddocs %}

{% docs spapi_orders %}
**SP-API order items** flattened to one row per (`order_id`, `asin`).  
Includes `qty`, `item_price`, `order_status`, `marketplace_id` and `purchase_ts`.  
Primary input for sales reporting and joins to pricing/inventory models.
{% enddocs %}

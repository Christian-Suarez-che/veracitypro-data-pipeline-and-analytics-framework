-- previous-day window + labels (DST-safe via session timezone)
-- ensure dbt on-run-start sets: alter session set timezone = 'America/New_York';

{% macro vp_prev_day_bounds(as_of_date) %}
  -- previous calendar date as DATE (e.g., '2025-10-14' -> '2025-10-13')
  {% set d_prev = "dateadd(day,-1,to_date('" ~ as_of_date ~ "'))" %}
  {% set prev_date = d_prev ~ "::date" %}

  -- local start of previous day: 00:00:00 (TIMESTAMP_LTZ)
  {% set start_local = "to_timestamp_ltz(to_varchar(" ~ d_prev ~ ", 'YYYY-MM-DD') || ' 00:00:00')" %}

  -- local start of next day (midnight), then minus 1 second -> 23:59:59 previous day
  {% set next_mid_local = "to_timestamp_ltz(to_varchar(dateadd(day,1," ~ d_prev ~ "), 'YYYY-MM-DD') || ' 00:00:00')" %}
  {% set end_local = "dateadd(second,-1," ~ next_mid_local ~ ")" %}

  -- label alias for clarity; same instant as end_local
  {% set prev_day_end_ltz = end_local %}

  -- return snippets usable directly in WHERE/SELECT
  {{ return({
    "start": start_local,                 -- TIMESTAMP_LTZ 00:00:00 of prev day
    "end": end_local,                     -- TIMESTAMP_LTZ 23:59:59 of prev day
    "prev_date": prev_date,               -- DATE for prev day
    "prev_day_235959_ltz": prev_day_end_ltz  -- TIMESTAMP_LTZ 23:59:59 label
  }) }}
{% endmacro %}

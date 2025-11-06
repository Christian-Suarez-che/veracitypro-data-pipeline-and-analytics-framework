-- macros/keepa_time.sql
-- Summary:
-- - Converts Keepa "minutes since 2011-01-01" to Snowflake TIMESTAMP_NTZ.
-- - Centralized so time conversion is consistent in all models.

{% macro keepa_minutes_to_timestamp(min_expr) -%}
  -- 2011-01-01 00:00:00 UTC = UNIX seconds offset 1293840000.
  -- Keepa minutes -> seconds -> timestamp (NTZ to avoid TZ drift at rest).
  to_timestamp_ntz( ({{ min_expr }} + 21564000) * 60 )
  -- Note: 21564000 = minutes between 1970-01-01 and 2011-01-01.
{%- endmacro %}

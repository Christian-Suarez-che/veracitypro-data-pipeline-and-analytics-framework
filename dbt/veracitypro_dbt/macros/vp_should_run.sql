-- returns true if the stream has >= min_rows between start_ts and end_ts
-- NOTE: SELECTing from a stream does NOT advance its offset (only DML would).

{% macro vp_should_run(stream_name, start_ts, end_ts, ts_column='ingest_ts', min_rows=1) %}
  -- during parsing/compile, skip DB calls and assume true
  {% if not execute %} {{ return(true) }} {% endif %}

  -- build COUNT(*) query against the stream using the provided window
  {% set q -%}
    select count(*) as c
    from {{ stream_name }}
    where {{ ts_column }} between {{ start_ts }} and {{ end_ts }}
  {%- endset %}

  -- run query now; res is a dbt.result agate table (or None on failure)
  {% set res = run_query(q) %}

  -- defensively extract the count
  {% if res is not none and res.columns|length > 0 and res.columns[0].values()|length > 0 %}
    {% set n = res.columns[0].values()[0] %}
  {% else %}
    {% set n = 0 %}
  {% endif %}

  -- return boolean: true if we have enough rows to proceed
  {{ return( (n | int) >= (min_rows | int) ) }}
{% endmacro %}

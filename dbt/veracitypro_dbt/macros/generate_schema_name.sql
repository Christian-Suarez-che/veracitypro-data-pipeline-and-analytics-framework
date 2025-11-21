{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
      If no +schema is set on the model, use the target schema.
      If +schema is set (STG, CORE, MART, MONITORING), use that exactly
      with no target prefix.
    #}
    {% if custom_schema_name is none %}
        {{ target.schema }}
    {% else %}
        {{ custom_schema_name | upper }}
    {% endif %}
{%- endmacro %}
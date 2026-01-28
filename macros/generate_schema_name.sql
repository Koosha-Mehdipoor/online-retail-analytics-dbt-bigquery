{% macro generate_schema_name(custom_schema_name, node) -%}
  {# BigQuery dataset naming: use the folder/schema exactly, no prefix #}
  {% if custom_schema_name is none %}
    {{ target.schema }}
  {% else %}
    {{ custom_schema_name }}
  {% endif %}
{%- endmacro %}

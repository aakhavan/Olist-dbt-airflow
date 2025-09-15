{% macro to_dollars(column_name, decimal_places=2) %}
    round(cast(toFloat64({{ column_name }}) as decimal(16, {{ decimal_places }})), {{ decimal_places }})
{% endmacro %}
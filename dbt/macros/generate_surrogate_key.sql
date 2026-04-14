{% macro generate_surrogate_key(field_list) %}
    {%- set fields = [] -%}
    {%- for field in field_list -%}
        {%- set _ = fields.append("COALESCE(CAST(" ~ field ~ " AS VARCHAR), '_null_')") -%}
    {%- endfor -%}
    MD5(CONCAT_WS('||', {{ fields | join(', ') }}))
{% endmacro %}

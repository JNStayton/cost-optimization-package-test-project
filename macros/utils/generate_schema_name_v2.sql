{% macro generate_schema_name_v2(custom_schema_name, node) %}
    
    {% set use_custom_schema = env_var('DBT_USE_CUSTOM_SCHEMA', 'false') | lower %}

    {% set default_schema = target.schema %}

    {% if custom_schema_name is none %}

        {{ default_schema }}

    {% elif use_custom_schema == 'true' %}

        {{ custom_schema_name | trim }}

    {% else %}

        {{ default_schema }}

    {% endif %}

{% endmacro %}
{% macro generate_schema_name(custom_schema_name, node) %}
-- rely on dbt env variable to let us know which schema to use (custom or default)
-- set ONE env var DBT_USE_CUSTOM_SCHEMA with true or false for each env
    
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
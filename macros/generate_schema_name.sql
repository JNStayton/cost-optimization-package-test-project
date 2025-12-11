{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {% if custom_schema_name is none %}

        {{ default_schema }}

    {% elif target.name in ['prod', 'staging'] %}

        {{ custom_schema_name | trim }}

    {% elif target.name in ['default', 'dev'] %}

        {{ default_schema }}

    {% else %}

        {{ custom_schema_name | trim }}

    {% endif %}

{% endmacro %}
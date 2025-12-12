{% macro generate_schema_name(custom_schema_name, node) %}

    {% set default_schema = target.schema %}

    {% if custom_schema_name is none %}

        {{ default_schema }}

    {% elif target.name in ['prod', 'staging'] %}

        {{ custom_schema_name | trim }}

    {% elif target.name == 'dev' %}

        {{ default_schema }}

    {% endif %}

{% endmacro %}
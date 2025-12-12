-- rely on target to tell dbt which schema to use, custom or default
-- set target in each developer profile to 'dev' 
-- set target in each job to 'prod' or 'staging' 

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
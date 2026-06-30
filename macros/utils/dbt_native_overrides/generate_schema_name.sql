{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set default_schema = target.schema -%}
    {%- set custom_schema = custom_schema_name | lower if custom_schema_name is not none else none -%}

    {# this env var is set in dbt cloud and points to the environment tag (DEV, PROD, STG) #}
    {% if env_var('DBT_CLOUD_ENVIRONMENT_TYPE', '') == 'dev' %}
        {{ default_schema }}
    {# fall back to the default schema in the event no custom schema has been set#}
    {% elif custom_schema is none %}
        {{ default_schema }}
    {# if the invocation context is CI, we want to use the default schema to allow dbt to create and drop the PR schemas automatically #}
    {% elif env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'ci' %}
        {{ default_schema }}
    {# for all other envs and contexts, use the custom schema #}
    {% else %}
        {{ custom_schema | trim }}
    {% endif %}

{% endmacro %}
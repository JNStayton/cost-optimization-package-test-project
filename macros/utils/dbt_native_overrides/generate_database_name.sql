{% macro generate_database_name(custom_database_name, node) %}

    {%- set default_database = target.database -%}
    {%- set custom_database = custom_database_name | trim if custom_database_name is not none else none -%}

    {% if env_var('DBT_CLOUD_ENVIRONMENT_TYPE', '') == 'dev' %}
        {{ default_database }}
    {% elif env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'ci' %}
        {{ default_database }}
    {% elif custom_database is none %}
        {{ default_database }}
    {% else %}
        {{ custom_database }}
    {% endif %}

{% endmacro %}
{% macro generate_schema_name(custom_schema_name, node) %}
    
    {% set env_name = env_var('DBT_CLOUD_ENVIRONMENT_NAME') | lower %}
    {% set job_context = env_var('DBT_CLOUD_INVOCATION_CONTEXT') | lower %}

    {% set default_schema = target.schema %}

    {% if env_name == 'development' %}

        {{ default_schema }}

    {% elif job_context == 'ci' %}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {% elif env_name == 'prod' %}

        {{ custom_schema_name | trim }}

    {% else %}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {% endif %}

{% endmacro %}
{% macro reset_pr_schema() %}

    {% set pr_id = env_var("DBT_CLOUD_PR_ID", "") %}
    {% set job_id = env_var("DBT_CLOUD_JOB_ID", "") %}

    {% if pr_id == "" or job_id == "" %}
        {{
            log(
                "Not a CI run or env vars not set — skipping PR schema cleanup.",
                info=True,
            )
        }}
        {% do return(none) %}
    {% endif %}

    {% set pr_prefix = "dbt_cloud_pr_" ~ job_id ~ "_" ~ pr_id %}

    {% set find_schemas %}
    select schema_name
    from information_schema.schemata
    where schema_name ilike '{{ pr_prefix }}%'
    {% endset %}

    {% set schema_results = run_query(find_schemas) %}

    {% if execute %}
        {% if schema_results.rows | length == 0 %}
            {{
                log(
                    "No PR schemas found matching "
                    ~ pr_prefix
                    ~ " — skipping cleanup.",
                    info=True,
                )
            }}
            {% do return(none) %}
        {% endif %}

        {% for schema_row in schema_results.rows %}
            {% set pr_schema = schema_row[0] %}
            {{ log("Cleaning schema: " ~ pr_schema, info=True) }}

            {% set find_tables %}
        select table_name
        from information_schema.tables
        where table_schema = upper('{{ pr_schema }}')
          and table_type = 'BASE TABLE'
            {% endset %}

            {% set table_results = run_query(find_tables) %}

            {% for row in table_results.rows %}
                {% set drop_stmt = "drop table if exists " ~ pr_schema ~ "." ~ row[0] %}
                {{ log("Dropping: " ~ drop_stmt, info=True) }}
                {% do run_query(drop_stmt) %}
            {% endfor %}

        {% endfor %}

    {% endif %}

{% endmacro %}

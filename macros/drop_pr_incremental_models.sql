{% macro drop_pr_incremental_models() %}

  {% set pr_id = env_var('DBT_CLOUD_PR_ID', '') %}
  {% set job_id = env_var('DBT_CLOUD_JOB_ID', '') %}

  {% if pr_id == '' or job_id == '' %}
    {{ log("Not a CI run or env vars not set — skipping PR schema cleanup.", info=True) }}
    {% do return(none) %}
  {% endif %}

  {% set pr_schema = 'dbt_cloud_pr_' ~ job_id ~ '_' ~ pr_id %}

  {% set schema_exists %}
    select count(*) as schema_count
    from information_schema.schemata
    where schema_name = upper('{{ pr_schema }}')
  {% endset %}

  {% set schema_check = run_query(schema_exists) %}

  {% if execute %}
    {% if schema_check.rows[0][0] == 0 %}
      {{ log("PR schema " ~ pr_schema ~ " does not exist yet — skipping cleanup.", info=True) }}
      {% do return(none) %}
    {% endif %}

    {% set find_tables %}
      select table_name
      from information_schema.tables
      where table_schema = upper('{{ pr_schema }}')
        and table_type = 'BASE TABLE'
    {% endset %}

    {% set results = run_query(find_tables) %}

    {% for row in results.rows %}
      {% set drop_stmt = 'drop table if exists ' ~ pr_schema ~ '.' ~ row[0] %}
      {{ log("Dropping: " ~ drop_stmt, info=True) }}
      {% do run_query(drop_stmt) %}
    {% endfor %}

  {% endif %}

{% endmacro %}
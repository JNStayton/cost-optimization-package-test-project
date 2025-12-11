{% macro schema_cleanup(preview_only=True, database=target.database, schema=target.schema) %}
  
  {#
    Usage: Drops tables and views in a target schema that are not found 
    in the current dbt project's graph. This is to help keep the development schemas clean.

    Arguments:
      - preview_only (boolean): 
        - If True, prints DROP statements without executing them. 
        - If False, executes the DROP statements. Defaults to True.
      - database (string): The database to clean. Defaults to the target database. Input your development database.
      - schema (string): The schema to clean. Defaults to the target schema. Input your development schema.

    Usage:
      # preview_only: RUN AND VERIFY BEFORE EXECUTING!
      dbt run-operation schema_cleanup --args '{"database": "dev_db", "schema": "dev_schema"}'

      # Execute: (Requires preview_only=False)
      dbt run-operation schema_cleanup --args '{"preview_only": False, "database": "dev_db", "schema": "dev_schema"}'
  #}
  
  {% if execute %}
    
    {% set current_relations = [] %}
    
    {# current dbt project models  #}
    {% for node in graph.nodes.values() | selectattr('resource_type', 'in', ['model', 'seed', 'snapshot']) %}
      {% set relation_name = node.config.alias or node.name %} 
      {% do current_relations.append(relation_name | upper) %} 
    {% endfor %}
    
    -- uncomment to log the current dbt project models
    -- {{ log("Current relations in dbt project: " ~ current_relations, info=True) }}

    {# identify non-project tables/views in the given database and schema #}
    {% set get_drop_relations_sql %}
      select
        'drop ' || case when table_type = 'VIEW' then 'view' else '
        table' end || 
        ' if exists ' || table_catalog || '.' || table_schema || '.' || table_name || ';' as drop_statement
      from {{ database }}.information_schema.tables
      where 
        table_schema = upper('{{ schema }}') 
        and table_catalog = upper('{{ database }}')
        and table_name not in ('{{ current_relations | join("','") }}')
    {% endset %}
    
    -- uncomment to log the above SQL statement for debugging if needed
    -- {{ log("SQL to identify old relations:\n" ~ get_drop_relations_sql, info=True) }}

    {# execute above SQL to compile the drop statements #}
    {% set results = run_query(get_drop_relations_sql) %}
    
    {# loop through and print/execute the drop statements #}
    {% if results and results.columns %}
      {% set drop_statements = results.columns[0].values() %}
      
      {% for drop_statement in drop_statements %}
        
        {% if preview_only %}
          {{ log("Compiled drop statements: ", info=True) }}
          {{ log(drop_statement, info=True) }}
        {% else %}
          {{ log("[EXECUTING] " ~ drop_statement, info=True) }}
          {% do run_query(drop_statement) %}
        {% endif %}
        
      {% endfor %}
      
      {% if drop_statements | length > 0 %}
        {{ log("Cleanup complete. " ~ drop_statements | length ~ " old relations processed.", info=True) }}
      {% else %}
        {{ log("Schema is clean. No old relations found to drop.", info=True) }}
      {% endif %}

    {% else %}
      {{ log("No relations found in target schema.", info=True) }}
    {% endif %}

  {% endif %}

{% endmacro %}
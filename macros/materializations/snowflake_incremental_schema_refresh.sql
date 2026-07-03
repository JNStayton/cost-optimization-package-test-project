{#
  Custom Snowflake incremental materialization override
  Adds automatic full-refresh triggering on schema change detection

  DROP INTO: macros/materializations/snowflake_incremental_schema_refresh.sql

  HOW IT WORKS:
  Uses a custom config key `schema_refresh` (not `on_schema_change`) to avoid
  Fusion's parse-time enum validation on on_schema_change values. When
  schema_refresh='auto' is set, the materialization:

  1. Strips is_incremental() filter blocks from compiled_code using raw_code
     as a guide — producing clean_sql with no WHERE filter
  2. Runs a cheap dry-run (SELECT * ... WHERE false LIMIT 0) on clean_sql
     to infer the incoming column list from Snowflake without scanning data
  3. Gets existing table columns via get_columns_in_relation(existing_relation)
  4. Diffs them — if new or renamed columns detected, sets schema_triggered_full_refresh
  5. Builds tmp relation normally (only if NOT doing a schema-triggered full refresh)
  6. If schema change: full rebuild with clean_sql (no WHERE filter, all rows load)
     If no change: normal incremental merge path

  This approach:
  - Avoids building the tmp relation before detection (eliminates the 3+ min
    penalty of resolving a view that references a large table)
  - Uses clean_sql for the dry-run (no correlated aggregate / CTE scope errors)
  - Uses clean_sql for the full rebuild (no incremental filter, all rows load)
  - Requires no changes to how developers write their incremental filter blocks

  USAGE — add to any incremental model config:
    {{ config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='your_key',
        meta={'schema_refresh': 'auto'}
    ) }}

  NO special incremental filter pattern required — any valid is_incremental()
  block works. The strip_incremental_filters macro handles removal automatically.

  ACTIVATION OPTIONS (any one of these triggers detection):
    1. Per-model:  meta={'schema_refresh': 'auto'} in model config
    2. CI-wide:    DBT_CLOUD_INVOCATION_CONTEXT env var = 'ci'
                   (set automatically by dbt platform for CI jobs)
    3. Job-level:  --vars '{"schema_refresh": "auto"}' passed to a specific job

  IMPORTANT NOTES:
  - SQL-only (Python models bypass detection and run normally)
  - Detects ALL schema changes: additions, deletions, renames, and reorders
  - Compares full ordered column lists — any difference triggers a full refresh
  - YAML schema definitions are irrelevant — detection is SQL → warehouse only
  - For stream-based incremental models (stream_source / incr_stream pattern):
    those macros check should_full_refresh() internally, which will be false
    during a schema-triggered refresh. Test on standard merge incrementals first.
  - This is a POC — sandbox thoroughly before applying to production models.

  TESTING CHECKLIST:
  [ ] Stage 1: Run with no column change → confirm normal incremental, no rebuild
  [ ] Stage 2: Add a column to SELECT → confirm rebuild fires, all rows populated, fast
  [ ] Stage 3: Rename a column → confirm rebuild fires, all rows populated, fast
  [ ] Stage 4: Remove a column → confirm rebuild fires, all rows populated, fast
  [ ] Stage 5: Reorder columns → confirm rebuild fires, all rows populated, fast
  [ ] Stage 6: Run with --full-refresh flag explicitly → confirm native behavior unchanged
  [ ] Stage 7: Run without schema_refresh config → confirm normal incremental behavior
  [ ] Stage 8: Run with DBT_CLOUD_INVOCATION_CONTEXT=ci → confirm CI auto-detection
  [ ] Stage 9: Run with --vars '{"schema_refresh": "auto"}' → confirm job-level trigger

  SOURCE REFERENCE:
  Built against dbt-adapters / dbt-snowflake incremental.sql (post Sep 2025 migration)
  https://github.com/dbt-labs/dbt-snowflake/blob/main/dbt/include/snowflake/macros/materializations/incremental.sql
#}


{# ============================================================
   PRESERVE: tmp relation type helper (unchanged from source)
   ============================================================ #}

{% macro dbt_snowflake_get_tmp_relation_type(strategy, unique_key, language) %}
  {%- set tmp_relation_type = config.get('tmp_relation_type') -%}

  {% if language == "python" and tmp_relation_type is not none and tmp_relation_type != "table" %}
    {% do exceptions.raise_compiler_error(
      "Python models currently only support 'table' for tmp_relation_type but "
       ~ tmp_relation_type ~ " was specified."
    ) %}
  {% endif %}

  {%- if language != "sql" %}
    {{ return("table") }}
  {% endif %}

  {% if snowflake__is_catalog_linked_database(relation=config.model) %}
    {{ return("table") }}
  {% endif %}

  {% if strategy in ["delete+insert", "microbatch"] and tmp_relation_type is not none and tmp_relation_type not in ("table", "transient") and unique_key is not none %}
    {% do exceptions.raise_compiler_error(
      "In order to maintain consistent results when `unique_key` is not none,
      the `" ~ strategy ~ "` strategy only supports `table` or `transient` for `tmp_relation_type` but "
      ~ tmp_relation_type ~ " was specified."
    ) %}
  {% endif %}

  {% if tmp_relation_type == "table" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "view" %}
    {{ return("view") }}
  {% elif tmp_relation_type == "transient" %}
    {{ return("transient") }}
  {% elif strategy in ("default", "merge", "append", "insert_overwrite") %}
    {{ return("view") }}
  {% elif strategy in ["delete+insert", "microbatch"] and unique_key is none %}
    {{ return("view") }}
  {% else %}
    {{ return("table") }}
  {% endif %}

{% endmacro %}


{# ============================================================
   PRESERVE: tmp relation resolution (unchanged from source)
   ============================================================ #}

{% macro resolve_incremental_tmp_relation(tmp_relation) %}
  {{ return(adapter.dispatch('resolve_incremental_tmp_relation', 'dbt')(tmp_relation)) }}
{% endmacro %}

{% macro snowflake__resolve_incremental_tmp_relation(tmp_relation) %}
  {{ return(tmp_relation) }}
{% endmacro %}


{# ============================================================
   HELPER: strip is_incremental() filter blocks from compiled SQL

   Uses raw_code to find all {% if is_incremental() %}...{% endif %}
   blocks. For each block, splits on {{ }} Jinja expressions, escapes
   the literal parts, joins with a FQN regex pattern where Jinja
   expressions were, and removes the matching rendered block from
   compiled_code via re.sub.

   Uses namespace() so result persists across loop iterations —
   Jinja does not allow variable mutation within loop scope.

   Returns compiled_code with all is_incremental() blocks removed.
   Safe for any model structure including early filters, multiple
   filters across CTEs, and any {{ }} expression in the filter.
   ============================================================ #}

{% macro strip_incremental_filters(raw_code, compiled_code) %}

  {% set open_tag = '{%\\s*if is_incremental\\(\\)\\s*%}' %}
  {% set close_tag = '{%\\s*endif\\s*%}' %}

  {# Split raw_code on the open tag to isolate incremental blocks #}
  {% set outer_parts = modules.re.split(open_tag, raw_code) %}

  {# namespace allows result to be updated across loop iterations #}
  {% set ns = namespace(result=compiled_code) %}

  {% if outer_parts | length > 1 %}
    {% for i in range(1, outer_parts | length) %}

      {# Split on endif to isolate the inner block content #}
      {% set inner_parts = modules.re.split(close_tag, outer_parts[i]) %}
      {% set block = inner_parts[0] %}

      {# Split inner block on {{ }} Jinja expressions #}
      {% set parts = modules.re.split('{{[\\s\\S]*?}}', block) %}

      {# Escape each literal part for regex safety #}
      {% set escaped_parts = [] %}
      {% for part in parts %}
        {% do escaped_parts.append(modules.re.escape(part)) %}
      {% endfor %}

      {# Join escaped literal parts with FQN pattern where Jinja expressions were #}
      {# FQN pattern: database.schema.identifier with optional quoting #}
      {% set fqn_pattern = '[\\w\\"]+\\.[\\w\\"]+\\.[\\w\\"]+' %}
      {% set block_pattern = escaped_parts | join(fqn_pattern) %}

      {# Remove matched block from result, persisted via namespace #}
      {% set ns.result = modules.re.sub(block_pattern, '', ns.result) %}

    {% endfor %}
  {% endif %}

  {% do return(ns.result) %}

{% endmacro %}


{# ============================================================
   HELPER: dry-run column inference on clean SQL

   Wraps clean_sql (incremental filters already stripped) in a
   SELECT * ... WHERE false LIMIT 0 subquery so Snowflake resolves
   the column list without scanning any data and without any
   self-referencing subquery or correlated aggregate risk.

   Returns a list of uppercase column name strings.
   ============================================================ #}

{% macro get_schema_dry_run_columns(clean_sql) %}

  {% set dry_run_sql %}
    select * from (
      {{ clean_sql }}
    ) dbt_schema_dry_run_subq
    where false
    limit 0
  {% endset %}

  {% set results = run_query(dry_run_sql) %}
  {% do return(results.columns | map(attribute='name') | map('upper') | list) %}

{% endmacro %}


{# ============================================================
   MAIN: Snowflake incremental materialization override
   Identical to dbt-adapters source EXCEPT for the
   schema_triggered_full_refresh block marked with NEW below.
   ============================================================ #}

{% materialization incremental, adapter='snowflake', supported_languages=['sql', 'python'] -%}

  {% set original_query_tag = set_query_tag() %}

  {#-- Set vars --#}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] -%}

  {%- set identifier = this.name -%}
  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
  {%- set is_catalog_linked_db = snowflake__is_catalog_linked_database(relation=none, catalog_relation=catalog_relation) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='table',
      table_format=catalog_relation.table_format,
  ) -%}

  {% set existing_relation = load_relation(this) %}

  {#-- The temp relation will be a view (faster) or temp table, depending on upsert/merge strategy --#}
  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {% set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, language) %}

  {% if is_catalog_linked_db %}
    {% set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_type, catalog=catalog_relation.catalog_name, is_table=true) %}
  {% else %}
    {% set tmp_relation_object_type = 'table' if tmp_relation_type == 'transient' else tmp_relation_type %}
    {% set tmp_relation = make_temp_relation(this).incorporate(type=tmp_relation_object_type) %}
  {% endif %}
  {% set tmp_relation = resolve_incremental_tmp_relation(tmp_relation) %}

  {% set grant_config = config.get('grants') %}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {# ---- NEW: resolve schema_refresh activation ----
     Three ways to activate — any one is sufficient:
       1. Per-model:  meta={'schema_refresh': 'auto'} in model config
       2. CI-wide:    DBT_CLOUD_INVOCATION_CONTEXT env var resolves to 'ci'
       3. Job-level:  --vars '{"schema_refresh": "auto"}' passed to the job
  #}
  {% set meta_refresh = config.get('meta', {}).get('schema_refresh', 'off') %}
  {% set ci_refresh = env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'ci' %}
  {% set var_refresh = var('schema_refresh', 'off') == 'auto' %}
  {% set schema_refresh_active = (meta_refresh == 'auto' or ci_refresh or var_refresh)
      and existing_relation is not none
      and not full_refresh_mode
      and language == 'sql'
      and not is_catalog_linked_db %}

  {# ---- NEW: pre-tmp schema detection ----
     Runs BEFORE building the tmp relation to avoid paying the cost
     of resolving an incremental view against a large table only to
     immediately discard it.

     1. Strip incremental filter blocks from compiled_code → clean_sql
     2. Dry-run clean_sql to get incoming column list (no WHERE, no risk)
     3. Diff against existing table columns
     4. Set schema_triggered_full_refresh if new/renamed columns found
  #}
  {% set schema_triggered_full_refresh = false %}
  {% set clean_sql = compiled_code %}

  {% if schema_refresh_active %}

    {% set clean_sql = strip_incremental_filters(model['raw_code'], compiled_code) %}

    {% set incoming_cols = get_schema_dry_run_columns(clean_sql) %}
    {% set existing_cols = adapter.get_columns_in_relation(existing_relation)
                           | map(attribute='name') | map('upper') | list %}

    {# Compare full ordered column lists — detects additions, deletions, renames, and reorders #}
    {% if incoming_cols != existing_cols %}
      {% do log('schema_refresh=auto: schema change detected in ' ~ this, info=True) %}
      {% do log('Incoming columns: ' ~ incoming_cols, info=True) %}
      {% do log('Existing columns: ' ~ existing_cols, info=True) %}
      {% do log('Triggering automatic full refresh.', info=True) %}
      {% set schema_triggered_full_refresh = true %}
    {% else %}
      {% do log(
        'schema_refresh=auto: no schema changes detected in ' ~ this ~ '. Running incrementally.', info=True) %}
    {% endif %}

  {% endif %}
  {# ---- END NEW ---- #}


  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif full_refresh_mode %}
    {#-- Native --full-refresh flag --#}
    {% if target_relation.needs_to_drop(existing_relation) %}
      {{ drop_relation_if_exists(existing_relation) }}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

  {% elif schema_triggered_full_refresh %}
    {#-- Schema change detected: full rebuild with clean_sql (no incremental filter) --#}
    {#-- No tmp relation was built so nothing to drop here --#}
    {% if target_relation.needs_to_drop(existing_relation) %}
      {{ drop_relation_if_exists(existing_relation) }}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, clean_sql, language) }}
    {%- endcall -%}

  {% elif target_relation.table_format != existing_relation.table_format %}
    {% do exceptions.raise_compiler_error(
      "Unable to update the incremental model `" ~ target_relation.identifier
      ~ "` from `" ~ existing_relation.table_format ~ "` to `"
      ~ target_relation.table_format
      ~ "` due to Snowflake limitation. Please execute with --full-refresh to drop the table and recreate in the new catalog."
    ) %}

  {% else %}
    {#-- Normal incremental path: build tmp and merge --#}
    {% if is_catalog_linked_db %}
      {%- call statement('create_tmp_relation', language=language) -%}
        {{ create_table_as(False, tmp_relation, compiled_code, language) }}
      {%- endcall -%}
    {% elif tmp_relation_type == 'view' %}
      {%- call statement('create_tmp_relation') -%}
        {{ snowflake__create_view_as_with_temp_flag(tmp_relation, compiled_code, True) }}
      {%- endcall -%}
    {% elif tmp_relation_type == 'transient' %}
      {%- call statement('create_tmp_relation', language=language) -%}
        {{ snowflake__create_table_transient_sql(tmp_relation, compiled_code) }}
      {%- endcall -%}
    {% else %}
      {%- call statement('create_tmp_relation', language=language) -%}
        {{ create_table_as(True, tmp_relation, compiled_code, language) }}
      {%- endcall -%}
    {% endif %}

    {% do adapter.expand_target_column_types(
        from_relation=tmp_relation,
        to_relation=target_relation
    ) %}

    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({
        'target_relation': target_relation,
        'temp_relation': tmp_relation,
        'unique_key': unique_key,
        'dest_columns': dest_columns,
        'incremental_predicates': incremental_predicates,
        'catalog_relation': catalog_relation
    }) %}

    {%- call statement('main') -%}
      {{ strategy_sql_macro_func(strategy_arg_dict) }}
    {%- endcall -%}

  {% endif %}

  {% do drop_relation_if_exists(tmp_relation) %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% set should_revoke = should_revoke(existing_relation.is_table, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{# ============================================================
   PRESERVE: default incremental strategy (unchanged from source)
   ============================================================ #}

{% macro snowflake__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}
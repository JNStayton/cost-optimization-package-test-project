{#
  snowflake_incremental_schema_refresh.sql
  Custom Snowflake incremental materialization override — adds on_schema_drift detection.

  Documentation: see on_schema_drift.md
  Source reference: https://github.com/dbt-labs/dbt-core/blob/main/crates/dbt-loader/src/dbt_macro_assets/dbt-snowflake/macros/materializations/incremental.sql
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
   HELPER: strip_incremental_filters — see on_schema_drift.md
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
   HELPER: get_schema_dry_run_columns — see on_schema_drift.md
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
   Identical to dbt-fusion bundled source EXCEPT for the
   on_schema_drift detection block marked with NEW below.
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

  {# ---- NEW: resolve on_schema_drift activation ----

     Precedence: model-level > CI > job-level var

     Model-level (highest precedence):
       meta={'on_schema_drift': 'auto|fail|ignore'}

     CI-wide (requires explicit project var opt-in):
       vars: ci_on_schema_drift: 'auto|fail'
       Only activates when DBT_CLOUD_INVOCATION_CONTEXT=ci.
       Defaults to 'ignore' — teams must explicitly opt in.

     Job-level (lowest precedence):
       dbt run --vars '{"on_schema_drift": "auto|fail"}'

     A model with meta={'on_schema_drift': 'ignore'} always skips
     detection regardless of CI or job-level settings.
  #}
  {% set model_drift = config.get('meta', {}).get('on_schema_drift', 'ignore') %}

  {% set ci_drift = 'ignore' %}
  {% if env_var('DBT_CLOUD_INVOCATION_CONTEXT', '') == 'ci' %}
    {% set ci_drift = var('ci_on_schema_drift', 'ignore') %}
  {% endif %}

  {% set var_drift = var('on_schema_drift', 'ignore') %}

  {# Model-level wins; fall through to CI then job-level #}
  {% if model_drift != 'ignore' %}
    {% set on_schema_drift = model_drift %}
  {% elif ci_drift != 'ignore' %}
    {% set on_schema_drift = ci_drift %}
  {% else %}
    {% set on_schema_drift = var_drift %}
  {% endif %}

  {# ---- NEW: schema drift detection ----
     Runs BEFORE building the tmp relation to avoid the cost of
     resolving an incremental view against a large table only to
     immediately discard it.

     Only activates when:
       - on_schema_drift is not 'ignore'
       - the table already exists (not a first run)
       - --full-refresh hasn't already been passed explicitly
       - language is SQL (Python not supported for detection)
       - not a catalog-linked db (Iceberg)
  #}
  {% set schema_triggered_full_refresh = false %}
  {% set clean_sql = compiled_code %}

  {% set drift_detection_active = on_schema_drift != 'ignore'
      and existing_relation is not none
      and not full_refresh_mode
      and language == 'sql'
      and not is_catalog_linked_db %}

  {% if drift_detection_active %}

    {% set clean_sql = strip_incremental_filters(model['raw_code'], compiled_code) %}
    {% set incoming_cols = get_schema_dry_run_columns(clean_sql) %}
    {% set existing_cols = adapter.get_columns_in_relation(existing_relation)
                           | map(attribute='name') | map('upper') | list %}
    {% set drift_detected = (incoming_cols != existing_cols) %}

    {% if drift_detected %}

      {% do log('on_schema_drift=' ~ on_schema_drift ~ ': schema drift detected in ' ~ this, info=True) %}
      {% do log('Incoming columns: ' ~ incoming_cols, info=True) %}
      {% do log('Existing columns: ' ~ existing_cols, info=True) %}

      {% if on_schema_drift == 'auto' %}
        {% do log('Triggering automatic full refresh.', info=True) %}
        {% set schema_triggered_full_refresh = true %}

      {% elif on_schema_drift == 'fail' %}
        {% do exceptions.raise_compiler_error(
          'on_schema_drift=fail: schema drift detected in ' ~ this ~ '. '
          ~ 'Run with --full-refresh to resolve, or set on_schema_drift=auto '
          ~ 'to handle this automatically.'
        ) %}

      {% endif %}

    {% else %}

      {% do log(
        'on_schema_drift=' ~ on_schema_drift ~ ': no drift detected in ' ~ this
        ~ '. Running incrementally.',
        info=True
      ) %}

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
    {#-- Schema drift detected with on_schema_drift=auto --#}
    {#-- Full rebuild with clean_sql — incremental filter stripped so all rows load --#}
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
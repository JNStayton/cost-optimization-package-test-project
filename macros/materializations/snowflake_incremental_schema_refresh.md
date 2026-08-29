# on_schema_drift — schema drift detection for incremental models

`on_schema_drift` is a custom configuration for Snowflake incremental models that automatically detects when a model's column structure has diverged from its existing table in the warehouse, and responds according to a configurable policy.

It is implemented as a Snowflake incremental materialization override and two helper macros, dropped into your dbt project's `macros/` directory. No changes to existing models are required to use it — detection is fully opt-in.

---

## Installation

Drop the following files into your project:

```
macros/
  materializations/
    snowflake_incremental_schema_refresh.sql  ← materialization override + helper macros
    on_schema_drift_macros.yml                ← macro documentation
```

No `packages.yml` entry is needed. dbt will automatically pick up the materialization override and macros from your project's `macros/` directory.

**Requirements:**
- dbt-fusion 2.0.0-preview.196 or later
- Snowflake adapter
- SQL incremental models only (Python models are bypassed)

---

## How it works

When drift detection is active, the materialization runs the following steps **before** building the tmp relation:

1. **Strip incremental filters** — uses `raw_code` (the unrendered Jinja source) to locate all `{% if is_incremental() %}...{% endif %}` blocks and remove their rendered equivalents from `compiled_code`, producing `clean_sql` with no WHERE filter.

2. **Dry-run column inference** — runs `SELECT * FROM (...clean_sql...) WHERE false LIMIT 0` against Snowflake. This resolves the incoming column list from the query plan without scanning any data and without risk of correlated aggregate errors.

3. **Column comparison** — compares the full ordered incoming column list against the existing table's columns via `DESCRIBE TABLE`. Any difference — addition, deletion, rename, or reorder — is considered drift.

4. **Response** — acts according to the configured `on_schema_drift` value (see below).

By running detection **before** the tmp relation is built, the materialization avoids the cost of resolving an incremental view against a large table only to immediately discard it.

---

## Configuration

`on_schema_drift` is set via the model's `meta` config to avoid dbt Fusion's parse-time validation of `on_schema_change` values.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    meta={'on_schema_drift': 'auto'}
) }}
```

### Values

| Value | Behavior |
|---|---|
| `ignore` | Default. No detection runs. Model behaves as a standard Snowflake incremental. |
| `auto` | Detects drift and automatically triggers a full rebuild. All rows load correctly. |
|  | Detects drift and raises a compiler error (`JinjaError`). The model fails immediately. No warehouse compute is used beyond the dry-run. |

### What counts as drift

The full ordered column list is compared. Any of the following triggers the configured response:

- Column added
- Column deleted  
- Column renamed
- Column reordered

### What does NOT trigger drift detection

- `on_schema_drift` not set (defaults to `ignore`)
- First run of the model (no existing table)
- `--full-refresh` flag passed explicitly (native full refresh takes precedence)
- Python models (bypassed entirely)
- Iceberg / catalog-linked database models (bypassed)

---

## Activation options

Detection can be activated at three levels. Precedence runs from highest to lowest: **model-level > CI > job-level var**.

### 1. Per-model (highest precedence)

Set directly in the model config:

```sql
meta={'on_schema_drift': 'auto'}   -- or 'fail', 'ignore'
```

A model with `meta={'on_schema_drift': 'ignore'}` will always skip detection regardless of CI or job-level settings.

### 2. CI-wide

Activate for all incremental models during CI runs. Requires **explicit opt-in** via a project variable — it does not activate automatically.

```yaml
# dbt_project.yml
vars:
  ci_on_schema_drift: 'auto'   # or 'fail'
```

This activates when `DBT_CLOUD_INVOCATION_CONTEXT=ci`, which is set automatically by the dbt platform for all CI job runs. It has no effect in non-CI runs.

**CI-wide detection is off by default.** Teams must explicitly set `ci_on_schema_drift` to enable it.

### 3. Job-level (lowest precedence)

Pass as a variable to any dbt run:

```bash
dbt run --select my_model --vars '{"on_schema_drift": "auto"}'
```

Useful for a dedicated schema drift detection job that runs separately from production jobs. Production runs remain unaffected unless the var is explicitly passed.

---

## Relationship to `on_schema_change`

`on_schema_drift` and `on_schema_change` are independent and can coexist on the same model. They operate at different points in the incremental lifecycle:

| Config | When it runs | What it does |
|---|---|---|
| `on_schema_drift` | Before tmp relation is built | Detects drift and decides whether to full rebuild or continue |
| `on_schema_change` | During the incremental merge step | Handles column mismatches between the tmp relation and target table |

When `on_schema_drift=auto` triggers a full rebuild, `on_schema_change` is irrelevant — no merge occurs. When `on_schema_drift=fail` or `on_schema_drift=ignore`, the normal incremental path runs and `on_schema_change` applies as usual.

**Note:** Interactions between `on_schema_drift` and non-default `on_schema_change` values (`append_new_columns`, `sync_all_columns`) have not been fully tested as part of this POC. Use with care and test in a sandbox environment.

---

## Tested scenarios

All tests run on dbt-fusion 2.0.0-preview.196 / Snowflake, using a standard merge incremental model backed by TPCH SF100 data.

| Scenario | Result | Notes |
|---|---|---|
| No column change | Normal incremental | ~5s, no detection overhead |
| Column addition | Full refresh | All rows populated, new column present |
| Column rename | Full refresh | All rows populated, new name present |
| Column deletion | Full refresh | All rows populated, deleted column gone |
| Column reorder | Full refresh | All rows populated, new order reflected |
| Native `--full-refresh` | Native behavior | At timing parity with no override |
| No `on_schema_drift` config | Normal incremental | Detection completely bypassed |
| CI run, no schema change | Normal incremental | `ci_on_schema_drift` var set, no drift found |
| CI run, column addition | Full refresh | `DBT_CLOUD_INVOCATION_CONTEXT=ci` + `ci_on_schema_drift: auto` |
| `--vars '{"on_schema_drift": "auto"}'` | Full refresh | Job-level trigger, no model config needed |
| , drift present | Errored in 0.28s | `JinjaError` raised, no warehouse compute beyond dry-run |

---

## Known limitations

**Stream-based incremental models** — models using the `stream_source` / `incr_stream` custom macro pattern have not been tested. Those macros check `should_full_refresh()` internally, which returns `false` during a schema-triggered refresh since the global flag is not set. Behavior is untested and may be incorrect.

**Fusion parse-time validation** — dbt Fusion validates `on_schema_change` values at parse time in Rust before any Jinja executes. Adding `full_refresh` as a valid `on_schema_change` value through a project-level macro override is not possible in Fusion. This is why `on_schema_drift` uses `meta{}` as its config carrier rather than being a top-level config key. A native implementation would require engine-level support.

**`modules.re` availability** — the `strip_incremental_filters` macro uses `modules.re` (minijinja_contrib regex module). This is available in dbt-fusion 2.0.0-preview.196+ but is not available in dbt Core (Python). This POC is Fusion-only.

**`on_schema_change` interaction** — combinations of `on_schema_drift` with non-default `on_schema_change` values are out of scope for this POC and have not been tested.

**Python models** — bypassed entirely. Detection does not run for Python incremental models.

---

## Helper macros

### `strip_incremental_filters(raw_code, compiled_code)`

Removes all `{% if is_incremental() %}...{% endif %}` blocks from compiled SQL. See `on_schema_drift_macros.yml` for full documentation.

### `get_schema_dry_run_columns(clean_sql)`

Infers the column list of a SQL query via a zero-cost dry-run (`WHERE false LIMIT 0`). Must be called with clean SQL (incremental filters already stripped). See `on_schema_drift_macros.yml` for full documentation.

---

## GitHub issues

This POC addresses a long-standing request in dbt Core. Relevant open issues:

- [dbt-core #320](https://github.com/dbt-labs/dbt-core/issues/320) — original request for `on_schema_change: full_refresh` (open since 2017)
- [dbt-core #4473](https://github.com/dbt-labs/dbt-core/issues/4473) — explicit proposal for `on_schema_change: full_refresh` with discussion of why it is architecturally difficult (closed as stale)
- [dbt-fusion #1532](https://github.com/dbt-labs/dbt-core/issues/1532) — Fusion-specific proposal for `on_breaking_change: rebuild | fail | warn`

---

## Files

| File | Description |
|---|---|
| `macros/materializations/snowflake_incremental_schema_refresh.sql` | Materialization override + helper macros |
| `macros/materializations/on_schema_drift_macros.yml` | Macro documentation YAML |
# Product enhancement proposal: `on_schema_change: full_refresh` and native schema drift detection

**Author:** Jessica Stayton, Resident Architect  
**Date:** July 2026  
**Status:** POC complete, proposing native implementation  
**Cross-client interest:** WebstaurantStore (POC test bed), FCB, Sprout Social

---

## Summary

This proposal requests a native implementation of schema drift detection and automatic full-refresh triggering for incremental models in dbt. The community has requested this since 2017, and it remains one of the most consistently upvoted open issues in dbt Core. A working proof-of-concept has been built, tested, and validated on Snowflake with dbt-fusion 2.0.0-preview.196.

The POC demonstrates that the capability is buildable today as a custom materialization override. This proposal outlines what a native implementation should look like, where it belongs in the product, and a suggested roadmap for extending it across adapters.

---

## The problem

Incremental models accumulate schema debt silently. When a developer adds, removes, renames, or reorders a column in an incremental model's SELECT, none of the existing `on_schema_change` values handle the situation gracefully without manual intervention. Depending on the platform and strategy combination, the result is one of these:

- **The job fails with a confusing error.** A merge or insert fails because the tmp relation and the existing table have a column mismatch. The error message doesn't tell the developer this is a schema drift issue or how to resolve it.
- **The job produces incomplete results.** On some platforms and strategies, certain `on_schema_change` values silently omit columns from the insert or continue incrementally with a partial schema. No error fires; data quality suffers quietly.
- **The developer manually runs `--full-refresh`.** Which works, but requires catching the problem first, understanding it, and taking a manual action. In production pipelines this causes delays and can block downstream models.

None of the current `on_schema_change` configs are able to gracefully handle schema changes that require a full rebuild without manual intervention. `on_schema_change` handles the merge step behavior after the fact; it doesn't detect drift proactively.

### The CI pain point

Schema drift creates a particularly painful experience in CI. When a developer opens a PR that modifies an incremental model's columns, the CI job runs against the PR schema, which already has a table built from the previous run of that branch. If the schema has changed since that table was last built, the CI job fails or produces incorrect results. The fix requires:

- Manually dropping the incremental model's object in the PR schema before re-running CI, or
- Adding a `--full-refresh` flag to the CI job for the affected model, or
- Ignoring the CI failure and hoping production doesn't hit the same issue

With native schema drift detection, the model detects the mismatch automatically and rebuilds itself within the CI run. No manual cleanup, no stale PR schema objects carrying over between runs, no developer time spent diagnosing a non-obvious error.

### Community signal

This is one of the most long-standing open requests in dbt Core:

- **[dbt-core #320](https://github.com/dbt-labs/dbt-core/issues/320):** original request for `on_schema_change: full_refresh`, open since 2017
- **[dbt-core #4473](https://github.com/dbt-labs/dbt-core/issues/4473):** explicit proposal for `on_schema_change: full_refresh` with extended discussion of architectural challenges; closed as stale without resolution
- **[dbt-fusion #1532](https://github.com/dbt-labs/dbt-core/issues/1532):** Fusion-specific proposal for `on_breaking_change: rebuild | fail | warn`

The request has been active for nearly a decade across multiple versions of dbt and multiple adapter teams. It consistently surfaces from analytics engineers managing large incremental pipelines.

---

## The POC

A working implementation has been built as a custom Snowflake incremental materialization override and two helper macros. It introduces a new config called `on_schema_drift` with three values: `auto`, `fail`, and `ignore` (default).

### How detection works

Before building the tmp relation, the materialization:

1. Strips `is_incremental()` filter blocks from compiled SQL, producing a clean version with no WHERE filter
2. Runs a zero-cost dry-run (`SELECT * FROM (...) WHERE false LIMIT 0`) to infer the incoming column list from Snowflake's query planner without scanning any data
3. Compares the full ordered incoming column list against the existing table's columns via `DESCRIBE TABLE`
4. If any difference is detected (addition, deletion, rename, or reorder), responds according to the configured value

By running detection before the tmp relation is built, the materialization avoids the cost of resolving an incremental view against a large table only to discard it.

### Config values

| Value | Behavior |
|---|---|
| `ignore` | Default. No detection runs. Fully backward compatible. |
| `auto` | Detects drift, strips the incremental filter, runs a full rebuild. All rows load correctly. |
| `fail` | Detects drift, raises a compiler error immediately (0.28s). No warehouse compute beyond the dry-run. |

### Activation options

Detection can be activated at three levels with explicit precedence (model > CI > job):

1. **Per-model:** `meta={'on_schema_drift': 'auto'}` in the model config
2. **CI-wide:** `ci_on_schema_drift: 'auto'` in `dbt_project.yml` vars, activates when `DBT_CLOUD_INVOCATION_CONTEXT=ci`; off by default, requires explicit opt-in
3. **Job-level:** `--vars '{"on_schema_drift": "auto"}'` passed to a specific run

A model can explicitly set `meta={'on_schema_drift': 'ignore'}` to opt out of CI or job-level triggers.

### Test results

All scenarios tested on dbt-fusion 2.0.0-preview.196, Snowflake, TPCH SF100 dataset:

| Scenario | Result |
|---|---|
| No column change | Normal incremental, ~5s, no detection overhead |
| Column addition | Full refresh fires, all rows populated |
| Column rename | Full refresh fires, all rows populated |
| Column deletion | Full refresh fires, deleted column gone |
| Column reorder | Full refresh fires, new order reflected |
| Native `--full-refresh` | At parity with no override |
| No `on_schema_drift` config | Normal incremental, detection completely bypassed |
| CI run without schema change | Normal incremental |
| CI run with column addition | Full refresh fires via CI env var |
| `--vars` job-level trigger | Full refresh fires without model config |
| `on_schema_drift=fail` with drift | Compiler error in 0.28s, no warehouse compute |

### POC files

- `macros/materializations/snowflake_incremental_schema_refresh.sql`
- `macros/materializations/on_schema_drift_macros.yml`
- `on_schema_drift.md` (user documentation)

---

## Why a native implementation is needed

The POC works and is usable today, but has three limitations that require a native solution.

### 1. Fusion parse-time validation blocks top-level config keys

dbt Fusion validates `on_schema_change` values at parse time in Rust before any Jinja executes. Adding `full_refresh` as a valid value through a project-level macro override isn't possible. This is why the POC uses `meta{'on_schema_drift': ...}` as a workaround. A native implementation should expose this as a first-class config key, ideally by extending `on_schema_change` itself.

### 2. `is_incremental()` evaluates at compile time

When `is_incremental()` evaluates to `true`, the incremental WHERE filter is baked into `compiled_code` before any materialization logic runs. The POC works around this by stripping the filter using regex on `raw_code`, which is fragile and requires `modules.re` (minijinja_contrib), available only in Fusion and not dbt Core. A native implementation should defer `is_incremental()` evaluation when schema drift detection is active, or expose a non-incremental compiled SQL path directly.

### 3. The `modules.re` dependency limits compatibility

The `strip_incremental_filters` macro relies on `modules.re`, which is available in dbt-fusion 2.0.0-preview.196+ but not in dbt Core (Python). A native implementation wouldn't need string manipulation at all; the engine would handle the column diff and execution path natively.

---

## Proposed native implementation

### Preferred approach: extend `on_schema_change`

Rather than introducing a separate config, the cleanest user experience extends the existing `on_schema_change` config with a new value:

```sql
{{ config(
    materialized='incremental',
    on_schema_change='full_refresh'
) }}
```

This is exactly what the community has been requesting since 2017. It fits naturally into the existing mental model: developers already know `on_schema_change`; adding `full_refresh` as a valid value requires no new concept to learn.

The full value set would be:

| Value | Existing/New | Behavior |
|---|---|---|
| `ignore` | Existing | No action on schema change (default) |
| `fail` | Existing | Fail the model on schema change |
| `append_new_columns` | Existing | Add new columns via ALTER TABLE |
| `sync_all_columns` | Existing | Sync all columns via ALTER TABLE |
| `full_refresh` | New | Detect drift, rebuild from scratch |

### Engine-level implementation path

The key insight from the POC: the architectural challenge is that `is_incremental()` evaluates at compile time before warehouse queries can run. The native fix requires one of two engine-level changes.

**Option A: Deferred `is_incremental()` evaluation**

Introduce a mechanism to defer `is_incremental()` evaluation until after a pre-execution schema check. When `on_schema_change: full_refresh` is set, Fusion would:

1. Compile the model twice: once with `is_incremental() = true` (incremental SQL) and once with `is_incremental() = false` (full SQL)
2. At execution time, run the schema check against the warehouse
3. Select which compiled SQL to execute based on the result

This is architecturally cleanest but requires Fusion to support lazy/deferred compilation for specific models.

**Option B: New `is_incremental_and_schema_stable()` macro**

A new first-class macro that Fusion evaluates lazily, deferred until execution time because it requires a warehouse round-trip. It returns `true` only when the model is running incrementally and no schema drift is detected.

```sql
{% if is_incremental_and_schema_stable() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

This is additive and opt-in; existing `is_incremental()` usage is unaffected. Fusion already has the concept of deferred evaluation for some operations, so this fits architecturally.

**Option C: Artifact-based detection (longer term)**

Rather than a warehouse round-trip, compare the current manifest's column definitions against the previous manifest passed via `--state`. If a column appears in the current manifest that wasn't in the previous one, flag the model for full refresh.

This requires zero warehouse queries at detection time, integrates naturally with Fusion's existing state comparison infrastructure (`state:modified`, `state:modified.breaking`), and fits the direction dbt 2.0 is already heading with state-aware orchestration. The limitation is that it requires YAML column documentation to be reliable and `--state` to be configured.

The product team's own proposal in [fusion #1532](https://github.com/dbt-labs/dbt-core/issues/1532) uses this direction. The implementation here would extend that work to incremental models more broadly, not just contracted models.

---

## Backward compatibility

A native implementation should guarantee full backward compatibility:

- The default value for `on_schema_change` remains `ignore`; no existing models are affected
- `full_refresh` is additive; teams must opt in explicitly
- Existing `on_schema_change` values (`append_new_columns`, `sync_all_columns`, `fail`) are unchanged
- No changes are required to model SQL for teams not opting in

---

## Adapter roadmap

The POC is Snowflake-only. A native implementation should be available across all major adapters. The detection mechanism (pre-execution column diff) is adapter-agnostic; the execution path (full rebuild) is already handled by the existing `full_refresh_mode` branch in each adapter's materialization.

Suggested rollout order based on client volume and architectural similarity:

| Phase | Adapters | Notes |
|---|---|---|
| 1 | Snowflake | POC already validated |
| 2 | BigQuery, Databricks | High client volume, standard incremental patterns |
| 3 | Redshift, Postgres | Lower volume, may need testing for DELETE+INSERT strategy |
| 4 | All remaining adapters | Via dbt-adapters base implementation |

The base incremental materialization in `dbt-adapters` already handles the `full_refresh_mode` branch. If detection logic lives in the base adapter and the full refresh execution path is already in place, Phase 4 may require minimal adapter-specific work.

---

## Interaction with `on_schema_change`

A question for the product team: should `on_schema_change: full_refresh` replace or complement the existing values?

The POC keeps them independent. The recommended approach for a native implementation is to make `full_refresh` a complete replacement for the other values when set: if `full_refresh` is configured, schema drift triggers a full rebuild and the merge step is skipped entirely. The other `on_schema_change` values apply only when `full_refresh` isn't set.

This keeps the mental model clean: `on_schema_change: full_refresh` means "if anything about the schema changes, rebuild from scratch." Developers who want additive-only behavior still use `append_new_columns`.

---

## Open questions for the product team

1. **Naming:** `on_schema_change: full_refresh` vs. a new top-level config (`on_schema_drift`) vs. the Fusion proposal naming (`on_breaking_change`). The POC used `on_schema_drift` as a workaround for Fusion's parse-time validation; for a native implementation, extending `on_schema_change` is preferred.

2. **Detection granularity:** should `full_refresh` trigger on any column change (the POC's behavior), or only on breaking changes (renames, deletions, reorders) but not additions? The community thread in #320 suggests additions are handleable by `append_new_columns`; breaking changes are where `full_refresh` adds the most value.

3. **CI-wide activation:** the POC's `ci_on_schema_drift` project var is a workaround. Native CI-wide activation via a project-level config or job setting would be more ergonomic. What is the right surface for this in the dbt platform?

4. **Downstream cascade:** if model A gets a schema-triggered full refresh and model B is a downstream incremental of model A, should B also be flagged for drift re-evaluation? The POC documents this as a known limitation. Fusion's DAG-aware compilation is well-positioned to handle this natively.

5. **Stream-based incrementals:** the POC was not tested with stream-based incremental models (e.g. Snowflake dynamic tables or the `stream_source` / `incr_stream` macro pattern). Those macros check `should_full_refresh()` internally and may need separate handling.

---

## Recommendation

Build `on_schema_change: full_refresh` as a first-class config value in the Snowflake adapter, using Option B (deferred `is_incremental_and_schema_stable()`) as the engine mechanism. Validate on Snowflake first, then extend to BigQuery and Databricks in a follow-on release. Treat the artifact-based detection approach (Option C) as the longer-term roadmap item, aligning with the state comparison infrastructure already being built in Fusion 2.0.

The POC code, test results, and full documentation are available and can be shared with the adapter team as a reference implementation.
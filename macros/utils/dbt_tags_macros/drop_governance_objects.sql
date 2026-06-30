{% macro drop_governance_objects(dry_run=false) %}

  {#
    drop_governance_objects
    ─────────────────────────────────────────────────────────────────────────
    Tears down all Snowflake governance objects created by
    create_governance_objects. Intended for use when migrating to a new
    governance structure, resetting a dev or test governance schema, or
    during environment teardown.

    Order of operations is enforced:
      1. Detach masking policies from tags
      2. Drop tags
      3. Drop masking policies

    Dropping a tag that still has a masking policy attached will fail in
    Snowflake. This macro handles detachment before dropping.

    Arguments:
      dry_run (bool, default: false)
        When true, logs all SQL that would be executed without running
        anything against Snowflake. The SHOW TAGS existence checks are
        skipped in dry run mode; all DROP and UNSET statements are logged
        as "would execute" regardless of current state.

    Required Snowflake privileges for the dbt role:
      - APPLY MASKING POLICY on account (to unset policies from tags)
      - OWNERSHIP or DROP privilege on tag objects in MASKING_DEMO.TAG_OBJECTS
      - OWNERSHIP or DROP privilege on masking policy objects in MASKING_DEMO.TAG_OBJECTS
      - USAGE on database MASKING_DEMO and schema TAG_OBJECTS

    Usage:
      dbt run-operation drop_governance_objects
      dbt run-operation drop_governance_objects --args '{"dry_run": true}'

    Warning:
      Dropping tags in production immediately removes masking policy
      enforcement from any column carrying those tags. Always run with
      dry_run=true first to confirm the scope of changes.
  #}

  {{ log("────────────────────────────────────────────", info=true) }}
  {% if dry_run %}
    {{ log("DRY RUN MODE — no SQL will be executed.", info=true) }}
    {{ log("SHOW TAGS checks are skipped; all DROP and UNSET statements", info=true) }}
    {{ log("are logged as 'would execute' regardless of current state.", info=true) }}
  {% else %}
    {{ log("Starting governance object teardown...", info=true) }}
  {% endif %}
  {{ log("────────────────────────────────────────────", info=true) }}


  -- ── Step 1: Detach masking policies from tags ────────────────────────────

  {#
    Idempotency check uses INFORMATION_SCHEMA.POLICY_REFERENCES rather than
    SHOW TAGS, which does not return a masking_policy column in its result
    set. POLICY_REFERENCES is real-time and scoped to the specific tag.
  #}

  {% set tag_policy_map = [
    { "tag": "pii_address", "policy": "mp_mask_address" },
    { "tag": "pii_phone",   "policy": "mp_mask_phone"   }
  ] %}

  {% for mapping in tag_policy_map %}

    {% set unset_sql %}
      ALTER TAG MASKING_DEMO.TAG_OBJECTS.{{ mapping.tag }}
        UNSET MASKING POLICY MASKING_DEMO.TAG_OBJECTS.{{ mapping.policy }};
    {% endset %}

    {% if dry_run %}
      {{ log(
        "[DRY RUN] Would execute (if tag exists and policy is attached):\n" ~ unset_sql,
        info=true
      ) }}

    {% else %}
      {% set check_sql %}
        SELECT POLICY_NAME
        FROM TABLE(
          MASKING_DEMO.INFORMATION_SCHEMA.POLICY_REFERENCES(
            REF_ENTITY_NAME => 'MASKING_DEMO.TAG_OBJECTS.{{ mapping.tag }}',
            REF_ENTITY_DOMAIN => 'TAG'
          )
        );
      {% endset %}
      {% set check_results = run_query(check_sql) %}
      {% set has_policy = (check_results.rows | length) > 0 %}

      {% if has_policy %}
        {% do run_query(unset_sql) %}
        {{ log(
          "Detached " ~ mapping.policy ~ " from tag: " ~ mapping.tag,
          info=true
        ) }}
      {% else %}
        {{ log(
          "Tag " ~ mapping.tag ~ " has no policy attached; skipping unset.",
          info=true
        ) }}
      {% endif %}

    {% endif %}

  {% endfor %}


  -- ── Step 2: Drop tags ────────────────────────────────────────────────────

  {% set tags = ["pii_address", "pii_phone"] %}

  {% for tag in tags %}

    {% set drop_tag_sql %}
      DROP TAG IF EXISTS MASKING_DEMO.TAG_OBJECTS.{{ tag }};
    {% endset %}

    {% if dry_run %}
      {{ log(
        "[DRY RUN] Would execute:\n" ~ drop_tag_sql,
        info=true
      ) }}

    {% else %}
      {% set show_sql %}
        SHOW TAGS LIKE '{{ tag }}' IN SCHEMA MASKING_DEMO.TAG_OBJECTS;
      {% endset %}
      {% set show_results = run_query(show_sql) %}

      {% if show_results | length == 0 %}
        {{ log("Tag not found; skipping drop: " ~ tag, info=true) }}
      {% else %}
        {% do run_query(drop_tag_sql) %}
        {{ log("Dropped tag: " ~ tag, info=true) }}
      {% endif %}

    {% endif %}

  {% endfor %}


  -- ── Step 3: Drop masking policies ────────────────────────────────────────

  {#
    Uses DROP IF EXISTS directly rather than a SHOW check. Scoped
    SHOW MASKING POLICIES LIKE filtering is not straightforward in
    Snowflake, and DROP IF EXISTS is a safe no-op if the policy
    doesn't exist.
  #}

  {% set policies = ["mp_mask_address", "mp_mask_phone"] %}

  {% for policy in policies %}

    {% set drop_policy_sql %}
      DROP MASKING POLICY IF EXISTS
        MASKING_DEMO.TAG_OBJECTS.{{ policy }};
    {% endset %}

    {% if dry_run %}
      {{ log(
        "[DRY RUN] Would execute:\n" ~ drop_policy_sql,
        info=true
      ) }}
    {% else %}
      {% do run_query(drop_policy_sql) %}
      {{ log("Dropped masking policy: " ~ policy, info=true) }}
    {% endif %}

  {% endfor %}


  {{ log("────────────────────────────────────────────", info=true) }}
  {% if dry_run %}
    {{ log("Dry run complete. No changes were made to Snowflake.", info=true) }}
  {% else %}
    {{ log("Governance teardown complete.", info=true) }}
  {% endif %}
  {{ log("────────────────────────────────────────────", info=true) }}

{% endmacro %}
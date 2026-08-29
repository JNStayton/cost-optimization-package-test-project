{% macro create_governance_objects(dry_run=false) %}

  {#
    create_governance_objects
    ─────────────────────────────────────────────────────────────────────────
    Creates all Snowflake governance objects required for tag-based column
    masking via the dbt-tags package. Includes tag objects, masking
    policies, and attachment of policies to tags.

    This macro is idempotent and safe to re-run at any time. Tags and
    masking policies are created with IF NOT EXISTS. Policy attachment uses
    a real-time SHOW TAGS check to determine whether a policy is already
    attached before attempting an ALTER TAG statement.

    This macro is intended to be run once during initial governance setup
    via dbt run-operation, not as part of a scheduled dbt build or job.

    Arguments:
      dry_run (bool, default: false)
        When true, logs all SQL that would be executed without running
        anything against Snowflake. The SHOW TAGS idempotency check is
        skipped in dry run mode; the ALTER TAG statements are always logged
        as "would execute" regardless of current state.

    Required Snowflake privileges for the dbt role:
      - CREATE TAG on schema MASKING_DEMO.TAG_OBJECTS
      - CREATE MASKING POLICY on schema MASKING_DEMO.TAG_OBJECTS
      - APPLY MASKING POLICY on account
      - APPLY TAG on each tag object (can be granted after tag creation)
      - USAGE on database MASKING_DEMO and schema TAG_OBJECTS

    Usage:
      dbt run-operation create_governance_objects
      dbt run-operation create_governance_objects --args '{"dry_run": true}'
  #}

  {{ log("────────────────────────────────────────────", info=true) }}
  {% if dry_run %}
    {{ log("DRY RUN MODE — no SQL will be executed.", info=true) }}
    {{ log("SHOW TAGS checks are skipped; ALTER TAG statements", info=true) }}
    {{ log("are logged as 'would execute' regardless of current state.", info=true) }}
  {% else %}
    {{ log("Starting governance object setup...", info=true) }}
  {% endif %}
  {{ log("────────────────────────────────────────────", info=true) }}


  -- ── Step 1: Create tag objects ──────────────────────────────────────────

  {% set tags = [
    {
      "name": "pii_address",
      "comment": "Applied to columns containing a physical address."
    },
    {
      "name": "pii_phone",
      "comment": "Applied to columns containing a phone number."
    }
  ] %}

  {% for tag in tags %}
    {% set tag_sql %}
      CREATE TAG IF NOT EXISTS
        MASKING_DEMO.TAG_OBJECTS.{{ tag.name }}
        COMMENT = '{{ tag.comment }}';
    {% endset %}

    {% if dry_run %}
      {{ log("[DRY RUN] Would execute:\n" ~ tag_sql, info=true) }}
    {% else %}
      {% do run_query(tag_sql) %}
      {{ log("Tag created (or already exists): " ~ tag.name, info=true) }}
    {% endif %}

  {% endfor %}


  -- ── Step 2: Create masking policies ─────────────────────────────────────

  {#
    Two policies covering the two example columns:
      - mp_mask_address: generic full redaction for physical addresses
      - mp_mask_phone:   partial mask returning a fixed ***-***-**** format

    Update the role list in CURRENT_ROLE() IN (...) to match your
    Snowflake RBAC structure before running.
  #}

  {% set address_policy_sql %}
    CREATE MASKING POLICY IF NOT EXISTS
      MASKING_DEMO.TAG_OBJECTS.mp_mask_address
    AS (val STRING) RETURNS STRING ->
      CASE
        WHEN CURRENT_ROLE() IN ('PII_ACCESS_ROLE', 'SYSADMIN')
          THEN val
        ELSE '**REDACTED**'
      END;
  {% endset %}

  {% if dry_run %}
    {{ log("[DRY RUN] Would execute:\n" ~ address_policy_sql, info=true) }}
  {% else %}
    {% do run_query(address_policy_sql) %}
    {{ log("Masking policy created (or already exists): mp_mask_address", info=true) }}
  {% endif %}

  {% set phone_policy_sql %}
    CREATE MASKING POLICY IF NOT EXISTS
      MASKING_DEMO.TAG_OBJECTS.mp_mask_phone
    AS (val STRING) RETURNS STRING ->
      CASE
        WHEN CURRENT_ROLE() IN ('PII_ACCESS_ROLE', 'SYSADMIN')
          THEN val
        ELSE '***-***-****'
      END;
  {% endset %}

  {% if dry_run %}
    {{ log("[DRY RUN] Would execute:\n" ~ phone_policy_sql, info=true) }}
  {% else %}
    {% do run_query(phone_policy_sql) %}
    {{ log("Masking policy created (or already exists): mp_mask_phone", info=true) }}
  {% endif %}


  -- ── Step 3: Attach masking policies to tags ──────────────────────────────

  {#
    Idempotency check uses INFORMATION_SCHEMA.POLICY_REFERENCES rather than
    SHOW TAGS. SHOW TAGS does not return a masking_policy column in its
    result set (confirmed against this account) — POLICY_REFERENCES is the
    correct, real-time (no ACCOUNT_USAGE latency) way to check whether a
    tag already has a policy attached.

    In dry run mode the POLICY_REFERENCES check is skipped entirely to keep
    this macro fully read-only. ALTER TAG statements are always logged as
    "would execute." The live run will perform the check and skip any tag
    that already has a policy attached.
  #}

  {% set tag_policy_map = [
    { "tag": "pii_address", "policy": "mp_mask_address" },
    { "tag": "pii_phone",   "policy": "mp_mask_phone"   }
  ] %}

  {% for mapping in tag_policy_map %}

    {% set attach_sql %}
      ALTER TAG MASKING_DEMO.TAG_OBJECTS.{{ mapping.tag }}
        SET MASKING POLICY MASKING_DEMO.TAG_OBJECTS.{{ mapping.policy }};
    {% endset %}

    {% if dry_run %}
      {{ log(
        "[DRY RUN] Would execute (if not already attached):\n" ~ attach_sql,
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
      {% set already_attached = (check_results.rows | length) > 0 %}

      {% if not already_attached %}
        {% do run_query(attach_sql) %}
        {{ log(
          "Attached " ~ mapping.policy ~ " to tag: " ~ mapping.tag,
          info=true
        ) }}
      {% else %}
        {{ log(
          "Tag " ~ mapping.tag ~ " already has a masking policy attached; skipping.",
          info=true
        ) }}
      {% endif %}

    {% endif %}

  {% endfor %}


  {{ log("────────────────────────────────────────────", info=true) }}
  {% if dry_run %}
    {{ log("Dry run complete. No changes were made to Snowflake.", info=true) }}
  {% else %}
    {{ log("Governance setup complete.", info=true) }}
  {% endif %}
  {{ log("────────────────────────────────────────────", info=true) }}

{% endmacro %}
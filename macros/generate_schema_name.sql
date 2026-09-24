{#
  Schema-name resolution override.

  WHY THIS EXISTS
  ---------------
  dbt's built-in behaviour is to CONCATENATE: a model configured with
  `+schema: raw` lands in `<target.schema>_raw`. With this project's prod
  profile (`schema: PUBLIC`) that produced `EXTERNAL_DATA.PUBLIC_RAW` for the
  external CLD staging models, when the agreed pipeline architecture calls the
  stage `RAW` (LANDING -> RAW -> NORMALISED -> SIGNALS; see
  loaders/sql/create_external_data_database.sql).

  WHY IT IS A POSITIVE ALLOWLIST, NOT A PLAIN OVERRIDE
  ----------------------------------------------------
  Every OTHER `+schema:` value in dbt_project.yml is authored as a SUFFIX that
  depends on that concatenation. Verified with `dbt ls --target prod` on main
  before this macro existed - the live schemas are:

      PUBLIC_LA_PRODUCT            (35 nodes)
      PUBLIC_LA_PRODUCT_STAGING    (12 nodes)
      PUBLIC_DBT_TEST__AUDIT       (61 nodes)
      PUBLIC_STAGING_ASC_HELPLINES (1 node)
      PUBLIC_ANALYTICS             (1 node)

  A conventional `generate_schema_name` that returns `custom_schema_name` with
  a fallback `else` would silently relocate ~60 models. `PUBLIC_LA_PRODUCT` is
  the grant boundary for the suppressed S3 export role (see
  admin/RBAC_ACCESS_MODEL.md and the marts.la_product comment in
  dbt_project.yml): moving it would create a new, ungranted schema while the
  granted one goes stale and keeps serving yesterday's data to consumers. That
  failure reads as a perfectly successful `dbt run`.

  So: this macro changes the answer for an explicit list of paths and for
  NOTHING else. The non-matching branch reproduces dbt's default exactly,
  including the `custom_schema_name is none` case and the `| trim`.

  RESOURCE TYPE GUARD
  -------------------
  Restricted to `resource_type == 'model'` deliberately. Generic tests
  declared on these models inherit an fqn under the same folder path, so
  without this guard a test would match the allowlist and have its audit
  schema resolved to `dbt_test__audit` instead of `PUBLIC_dbt_test__audit`.

  HOW TO VERIFY A CHANGE HERE
  ---------------------------
  Never edit this file without re-running the diff that justifies it:

      dbt ls --target prod --output json \
        --output-keys name schema database resource_type

  on main and on the branch, then diff. Acceptance is exact: ONLY the nodes
  you intend to move may differ. Run it against the PROD target - `dbt compile`
  under the dev target resolves against ANALYTICS_DEV and will not show you
  the schemas that actually matter.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {#- Folder paths whose models take their configured schema VERBATIM.
        Matched on node.fqn (deterministic at parse time), not on
        node.config.database - database resolution is a separate macro and
        config-inheritance order is not something to bet a grant boundary on. -#}
    {%- set verbatim_schema_paths = [
        ['staging', 'external'],
        ['normalised', 'external'],
    ] -%}

    {%- if node is not none and node.fqn is defined and node.fqn | length > 2 -%}
        {%- set node_path = node.fqn[1:-1] -%}
    {%- else -%}
        {%- set node_path = [] -%}
    {%- endif -%}

    {%- set is_model = node is not none
                       and node.resource_type is defined
                       and node.resource_type == 'model' -%}

    {%- if custom_schema_name is not none
           and is_model
           and node_path in verbatim_schema_paths -%}

        {{ custom_schema_name | trim }}

    {%- elif custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

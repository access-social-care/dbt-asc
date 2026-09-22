{#
  Hierarchy-level derivation for CLD breakdown columns.

  Every breakdown column in the CLD monthly sheets is HIERARCHICAL: the "All"
  row is a SUBTOTAL of the rows below it, not an extra category. Aggregating
  without filtering to a single level multiplies the true figure - by 2x for a
  simple All-plus-categories column, by 3x for age (all / coarse / fine) and
  for area_unit.

  All membership lists below were derived by summing the actual landed rows
  (England, 30 June 2025, sep2025 vintage, verified 2026-09-22), NOT read off
  the annual Tables 4-6. That distinction matters: the monthly sheets carry
  values the annual ones do not (support setting "Prison"; age bands
  "85 to 94" and "95 and above" where the annual tables have a single
  "85 and above"), so a list copied from the annual tables would mis-level
  real rows.

  UNRECOGNISED VALUES ARE NOT GUESSED AT. Anything not on a list below levels
  as 'unrecognised', and tests/assert_cld_breakdown_levels_known.sql fails on
  any such row. A future DHSC release that adds a band therefore breaks a test
  loudly instead of silently landing in whichever bucket a regex happened to
  match. Do not "fix" that test by widening a pattern - add the value here
  deliberately, after checking which level it actually sums into.
#}

{% macro cld_age_group_level(col) -%}
    CASE
        WHEN {{ col }} = 'All' THEN 'all'
        WHEN {{ col }} IN ('18 to 64', '65 and above') THEN 'coarse'
        WHEN {{ col }} IN (
            '18 to 24', '25 to 44', '45 to 64',
            '65 to 74', '75 to 84', '85 to 94', '95 and above'
        ) THEN 'fine'
        {#- "Unknown" is inside All but outside BOTH coarse and fine, so it
            is its own level rather than being folded into 'fine'. Summing
            fine + unknown does NOT reproduce All exactly either, because
            every published count is rounded to the nearest 5. -#}
        WHEN {{ col }} = 'Unknown' THEN 'unknown'
        ELSE 'unrecognised'
    END
{%- endmacro %}


{% macro cld_gender_level(col) -%}
    CASE
        WHEN {{ col }} = 'All' THEN 'all'
        {#- Verified exact, England Dec 2024:
            25,835 + 18,960 + 30 + 660 = 45,485 = the All row. -#}
        WHEN {{ col }} IN ('Female', 'Male', 'Other', 'Unknown') THEN 'category'
        ELSE 'unrecognised'
    END
{%- endmacro %}


{% macro cld_ethnicity_level(col) -%}
    CASE
        WHEN {{ col }} = 'All' THEN 'all'
        {#- Sub-groups are written "Group: Subgroup" (e.g. "White: Irish").
            The colon is the only structural marker the publisher gives, so
            it is what distinguishes the two levels. Six top-level groups sum
            to All exactly; the 21 sub-groups sum to All +/- rounding. -#}
        WHEN CONTAINS({{ col }}, ': ') THEN 'subgroup'
        WHEN {{ col }} IN (
            'White',
            'Asian or Asian British',
            'Black, Black British, Caribbean or African',
            'Mixed or multiple ethnic groups',
            'Other ethnic group',
            'No data'
        ) THEN 'group'
        ELSE 'unrecognised'
    END
{%- endmacro %}


{% macro cld_support_setting_level(col) -%}
    CASE
        {#- NULL, not 'unrecognised': the assessments file has no support
            setting dimension at all, so its rows carry a NULL here by
            design. Levelling that as 'unrecognised' would make the
            known-levels test fail on every assessments row. -#}
        WHEN {{ col }} IS NULL THEN NULL
        WHEN {{ col }} = 'All' THEN 'all'
        {#- Verified exact, England 30 Jun 2025:
            482,110 + 54,880 + 139,000 + 260 = 676,250 = the All row.
            "Prison" does NOT appear in the annual Tables 4-6. -#}
        WHEN {{ col }} IN (
            'Community', 'Nursing care', 'Residential care', 'Prison'
        ) THEN 'setting'
        ELSE 'unrecognised'
    END
{%- endmacro %}


{% macro cld_area_unit_level(col) -%}
    CASE
        WHEN {{ col }} = 'National' THEN 'national'
        WHEN {{ col }} = 'Region' THEN 'region'
        {#- ADASS regions are a SEPARATE, OVERLAPPING geography, not a
            subdivision of geographic regions: the source Notes sheet states
            that North East, North West, East of England and South East have
            different LA membership between the two. Levelled distinctly so
            nothing can sum 'region' and 'adass_region' together and count
            England twice. -#}
        WHEN {{ col }} = 'ADASS Region' THEN 'adass_region'
        WHEN {{ col }} = 'Local Authority' THEN 'local_authority'
        ELSE 'unrecognised'
    END
{%- endmacro %}

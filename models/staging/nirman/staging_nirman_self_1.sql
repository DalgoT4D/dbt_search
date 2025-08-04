{{ config(materialized='table') }}

{# 1️⃣ run_query to fetch table list only at run-time #}
{% if execute %}
  {% set tbls_query %}
    select table_name
      from information_schema.tables
     where table_schema = 'staging_nirman_workshop_data'
       and table_name ilike '%Self_1%'
     order by table_name
  {% endset %}
  {% set results = run_query(tbls_query) %}
  {% set tbls    = results.columns[0].values() %}
  {% if tbls | length == 0 %}
    {{ exceptions.raise_compiler_error(
         "No tables found in staging_nirman_workshop_data matching '%Self_1%'"
       ) }}
  {% endif %}
{% else %}
  {% set tbls = [] %}
{% endif %}

{# 2️⃣ Emit N SELECTs unioned together #}
{% for tbl in tbls %}
select
  '{{ tbl }}'            as table_name,
  to_jsonb(t.*)          as record_data
from staging_nirman_workshop_data."{{ tbl }}" as t
{% if not loop.last %} union all{% endif %}
{% endfor %}
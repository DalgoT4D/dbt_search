{{ config(materialized='table',
tags=["nirman"]) }}

with self_1_questions as (

  {% if execute %}
    {% set self1_query %}
      select table_name
      from information_schema.tables
      where table_schema = 'staging_nirman_workshop_data'
        and table_name ilike '%Self_1%'
      order by table_name
    {% endset %}

    {% set self1_results = run_query(self1_query) %}
    {% set self1_tables = self1_results.columns[0].values() %}
  {% else %}
    {% set self1_tables = [] %}
  {% endif %}

  {% for tbl in self1_tables %}
    select
      'Self_1'                  as source_type,
      t.q_no::text              as q_no,
      t.option_no::text         as option_no,
      t.questions::text         as questions,
      t.options::text           as options
    from staging_nirman_workshop_data."{{ tbl }}" as t
    where t.q_no is not null
      and t.option_no is not null
    {% if not loop.last %} union all {% endif %}
  {% endfor %}

),

self_2_questions as (

  {% if execute %}
    {% set self2_query %}
      select table_name
      from information_schema.tables
      where table_schema = 'staging_nirman_workshop_data'
        and table_name ilike '%Self_2%'
      order by table_name
    {% endset %}

    {% set self2_results = run_query(self2_query) %}
    {% set self2_tables = self2_results.columns[0].values() %}
  {% else %}
    {% set self2_tables = [] %}
  {% endif %}

  {% for tbl in self2_tables %}
    select
      'Self_2'                  as source_type,
      t.q_no::text              as q_no,
      t.option_no::text         as option_no,
      t.questions::text         as questions,
      t.options::text           as options
    from staging_nirman_workshop_data."{{ tbl }}" as t
    where t.q_no is not null
      and t.option_no is not null
    {% if not loop.last %} union all {% endif %}
  {% endfor %}

)

select distinct * from self_1_questions
union all
select distinct * from self_2_questions
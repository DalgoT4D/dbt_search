{{ config(materialized='table') }}

with experience_questions as (

  {% if execute %}
    {% set experience_query %}
      select table_name
      from information_schema.tables
      where table_schema = 'staging_nirman_workshop_feedback'
        and table_name ilike '%Experience%'
      order by table_name
    {% endset %}

    {% set experience_results = run_query(experience_query) %}
    {% set experience_tables = experience_results.columns[0].values() %}
  {% else %}
    {% set experience_tables = [] %}
  {% endif %}

  {% set filtered_tables = experience_tables | reject("equalto", "14___Experience") | list %}
  {% for tbl in filtered_tables %}
    select
      'Experience'              as source_type,
      t.s_n_::text              as q_no,
      coalesce(t.options_no::text, '1') as option_no,
      t.question::text          as questions,
      t.options::text           as options
    from staging_nirman_workshop_feedback."{{ tbl }}" as t
    where t.s_n_ is not null
    {% if not loop.last %} union all {% endif %}
  {% endfor %}

)

select distinct * from experience_questions
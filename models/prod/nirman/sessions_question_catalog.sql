{{ config(materialized='table',
tags=["nirman"]) }}

with sessions_questions as (

  {% if execute %}
    {% set sessions_query %}
      select table_name
      from information_schema.tables
      where table_schema = 'staging_nirman_workshop_feedback'
        and table_name ilike '%Sessions%'
      order by table_name
    {% endset %}

    {% set sessions_results = run_query(sessions_query) %}
    {% set sessions_tables = sessions_results.columns[0].values() %}
  {% else %}
    {% set sessions_tables = [] %}
  {% endif %}

  {% set filtered_sessions_tables = ["11_1_A_Sessions"] %}
  {% for tbl in filtered_sessions_tables %}
    select
      'Sessions'                as source_type,
      t.q_no::text              as q_no,
      t.option_no::text         as option_no,
      t.question::text          as questions,
      t.sessions::text          as options
    from staging_nirman_workshop_feedback."{{ tbl }}" as t
    where t.q_no is not null
      and t.option_no is not null
    {% if not loop.last %} union all {% endif %}
  {% endfor %}

)

select distinct * from sessions_questions
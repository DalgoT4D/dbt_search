{{ config(materialized='table') }}

with input as (
    select
        table_name,
        record_data ->> 'q_no'       as q_no,
        record_data ->> 'option_no'  as option_no,
        kv.key                       as key,
        kv.value                     as value
    from {{ ref('staging_nirman_sessions') }},
    lateral jsonb_each_text(record_data) as kv
),

filtered as (
    select
        table_name,
        q_no,
        option_no,
        key   as name,
        value as preference
    from input
    where key not in (
        'q_no', 'option_no', 'question', 'sessions', 'count', 'percentage',
        '_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta'
    )
    and q_no is not null and q_no != ''
    and option_no is not null and option_no != ''
    and value = '1'  -- Only include preferred sessions (marked with '1')
),

with_qo_key as (
    select
        -- Cleaned workshop name: e.g., "13_1_B_Sessions" -> "13 B - Sessions"
        regexp_replace(table_name, '^(\d+)_\d+_([A-Z])_(Sessions)$', '\1 \2 - \3') as workshop,

        -- Title-cased name: "om_shinde" -> "Om Shinde"
        initcap(replace(name, '_', ' ')) as name,

        -- Composite key like "q_1_o_2"
        concat('q_', q_no, '_o_', option_no) as qo_key,

        -- For sessions, preference is always 1 (selected)
        1 as preference_value
    from filtered
)

{% if execute %}
    {% set qo_query %}
        select distinct concat('q_', record_data ->> 'q_no', '_o_', record_data ->> 'option_no') as qo_key
        from {{ ref('staging_nirman_sessions') }}
        where record_data ? 'q_no' and record_data ? 'option_no'
              and nullif(record_data ->> 'q_no', '') is not null
              and nullif(record_data ->> 'option_no', '') is not null
    {% endset %}
    {% set qo_results = run_query(qo_query) %}
    {% set qo_values = qo_results.columns[0].values() %}
{% else %}
    {% set qo_values = [] %}
{% endif %}

select
    workshop,
    name,
    {{ dbt_utils.pivot(
        column='qo_key',
        values=qo_values | reject("equalto", "q__o_") | list,
        agg='max',
        then_value='preference_value'
    ) }}
from with_qo_key
where name is not null and name != ''
group by workshop, name
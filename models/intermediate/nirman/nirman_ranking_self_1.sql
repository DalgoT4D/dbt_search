{{ config(materialized='table',
tags=["nirman"]) }}

with input as (
    select
        table_name,
        record_data ->> 'q_no'       as q_no,
        record_data ->> 'option_no'  as option_no,
        kv.key                       as key,
        kv.value                     as value
    from {{ ref('staging_nirman_ranking_self_1') }},
    lateral jsonb_each_text(record_data) as kv
),

filtered as (
    select
        table_name,
        q_no,
        option_no,
        key   as name,
        value as score
    from input
    where key not in (
        'q_no', 'option_no', 'questions', 'options',
        '_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta'
    )
    and q_no is not null and q_no != ''
    and option_no is not null and option_no != ''
),

with_qo_key as (
    select
        -- Cleaned workshop name: e.g., "13_1_B_Self_1" -> "13 B"
        regexp_replace(table_name, '^(\d+)_\d+_([A-Z])_(Self_\d+)$', '\1 \2') as workshop,

        -- Title-cased name: "philemon_kuriakose" -> "Philemon Kuriakose"
        initcap(replace(name, '_', ' ')) as name,

        -- Composite key like "q_1_o_2"
        concat('q_', q_no, '_o_', option_no) as qo_key,

        -- Cast as NUMERIC instead of INT to allow decimals
        score::numeric
    from filtered
)

{% if execute %}
    {% set qo_query %}
        select distinct concat('q_', record_data ->> 'q_no', '_o_', record_data ->> 'option_no') as qo_key
        from {{ ref('staging_nirman_ranking_self_1') }}
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
        then_value='score'
    ) }}
from with_qo_key
group by workshop, name
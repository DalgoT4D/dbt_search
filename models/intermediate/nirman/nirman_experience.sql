{{ config(materialized='table',
tags=["nirman"]) }}

with input as (
    select
        table_name,
        record_data ->> 's_n_'       as q_no,
        record_data ->> 'options_no' as option_no,
        kv.key                       as key,
        kv.value                     as value
    from {{ ref('staging_nirman_experience') }},
    lateral jsonb_each_text(record_data) as kv
),

filtered as (
    select
        table_name,
        q_no,
        coalesce(option_no, '1') as option_no,  -- Default to '1' if options_no is null
        key   as name,
        value as score
    from input
    where key not in (
        's_n_', 'options_no', 'question', 'options', 'avg',
        'marked_1', 'marked_2', 'marked_3', 'marked_4', 'marked_5', 'marked_4_5', 'marked_1_to_3',
        '_airbyte_raw_id', '_airbyte_extracted_at', '_airbyte_meta'
    )
    and q_no is not null and q_no != ''
),

with_qo_key as (
    select
        -- Cleaned workshop name: e.g., "13_1_B_Experience" -> "13 B - Experience"
        regexp_replace(table_name, '^(\d+)_\d+_([A-Z])_(Experience)$', '\1 \2 - \3') as workshop,

        -- Title-cased name: "om_shinde" -> "Om Shinde"
        initcap(replace(name, '_', ' ')) as name,

        -- Composite key like "q_1_o_1"
        concat('q_', q_no, '_o_', option_no) as qo_key,

        -- Cast as NUMERIC instead of INT to allow decimals
        case 
            when score ~ '^[0-9]+\.?[0-9]*$' then score::numeric
            else null
        end as score
    from filtered
)

{% if execute %}
    {% set qo_query %}
        select distinct concat('q_', record_data ->> 's_n_', '_o_', coalesce(record_data ->> 'options_no', '1')) as qo_key
        from {{ ref('staging_nirman_experience') }}
        where record_data ? 's_n_'
              and nullif(record_data ->> 's_n_', '') is not null
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
where name is not null and name != ''
group by workshop, name
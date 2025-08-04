{{ config(materialized='table') }}

-- depends_on: {{ ref('staging_nirman_ranking_self_1') }}
-- depends_on: {{ ref('staging_nirman_ranking_self_2') }}

{% if execute %}
    {% set qo_query %}
        select distinct concat('q_', record_data ->> 'q_no', '_o_', record_data ->> 'option_no') as qo_key
        from {{ ref('staging_nirman_ranking_self_1') }}
        where record_data ? 'q_no' and record_data ? 'option_no'
              and nullif(record_data ->> 'q_no', '') is not null
              and nullif(record_data ->> 'option_no', '') is not null
        union
        select distinct concat('q_', record_data ->> 'q_no', '_o_', record_data ->> 'option_no') as qo_key
        from {{ ref('staging_nirman_ranking_self_2') }}
        where record_data ? 'q_no' and record_data ? 'option_no'
              and nullif(record_data ->> 'q_no', '') is not null
              and nullif(record_data ->> 'option_no', '') is not null
        order by qo_key
    {% endset %}
    {% set qo_results = run_query(qo_query) %}
    {% set qo_values = qo_results.columns[0].values() %}
{% else %}
    {% set qo_values = [] %}
{% endif %}

with base_data as (
    select
        coalesce(self1.workshop, self2.workshop) as workshop,
        coalesce(self1.name, self2.name) as name,
        
        -- Self1 columns
        {% for qo_key in qo_values | reject("equalto", "q__o_") | list %}
        self1.{{ qo_key }} as {{ qo_key }}_self1,
        {% endfor %}
        
        -- Self2 columns  
        {% for qo_key in qo_values | reject("equalto", "q__o_") | list %}
        self2.{{ qo_key }} as {{ qo_key }}_self2
        {%- if not loop.last -%},{%- endif %}
        {% endfor %}
        
    from {{ ref('nirman_ranking_self_1') }} self1
    full outer join {{ ref('nirman_ranking_self_2') }} self2
        on self1.workshop = self2.workshop 
        and self1.name = self2.name
),

with_improvements as (
    select
        *,
        
        -- Improvement calculations (self1 - self2, so positive = improvement)
        {% for qo_key in qo_values | reject("equalto", "q__o_") | list %}
        case 
            when {{ qo_key }}_self1 is not null and {{ qo_key }}_self2 is not null 
            then {{ qo_key }}_self1 - {{ qo_key }}_self2
            else null 
        end as {{ qo_key }}_improvement
        {%- if not loop.last -%},{%- endif %}
        {% endfor %}
        
    from base_data
)

select
    merged.*,
    demo.id as participant_id,
    demo.gender,
    demo.current_age,
    demo.native_area,
    demo.current_area,
    demo.native_state,
    demo.current_state,
    demo.native_district,
    demo.current_district,
    demo.educational_degree,
    demo.educational_stream,
    demo.speciality,
    demo.main_batch,
    demo.sub_batch,
    demo.workshops_attended,
    demo.current_domain_of_work,
    demo.current_status_of_work

from with_improvements merged
left join {{ ref('staging_nirman_master_participants') }} demo
    on lower(replace(merged.name, ' ', '_')) = demo.name_normalized
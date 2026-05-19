{{ config(materialized='table') }}

-- Stage 1: Extract raw questionnaire data using dynamic column generation
-- Column counts: carrer(8) + criteria(9) + insecurities(10) + social_contribution(1)
--              + question(142) + finance(5) = 175 response columns
with raw_questionnaire as (
    select
        participant_id,
        participant_name,
        workshop_name,
        workshop_phase,
        batch,

        -- Career columns (8 total: carrer_1 to carrer_8)
        -- Note: source has 'carrer' typo — preserved for compatibility
        {% for i in range(1, 9) %}
        carrer_{{ i }},
        {% endfor %}

        -- Criteria columns (9 total: criteria_1 to criteria_9)
        {% for i in range(1, 10) %}
        criteria_{{ i }},
        {% endfor %}

        -- Insecurities columns (10 total: insecurities_1 to insecurities_10)
        {% for i in range(1, 11) %}
        insecurities_{{ i }},
        {% endfor %}

        -- Social contribution (standardized values)
        case 
            when lower(trim(social_contribution)) in ('occasionally', 'occasionaly') then 'Occasionally'
            when lower(trim(social_contribution)) like 'part time%' then 'Part time'
            when lower(trim(social_contribution)) like '%monitory contribution%' then 'Monetary contribution'
            else social_contribution
        end as social_contribution,

        -- Question columns (142 total: question_1 to question_142)
        {% for i in range(1, 143) %}
        question_{{ i }},
        {% endfor %}

        -- Finance columns (5 total: finance_1 to finance_5)
        {% for i in range(1, 6) %}
        finance_{{ i }}{% if not loop.last %},{% endif %}
        {% endfor %}

    from {{ source('staging_nirman_questionnaire', 'staging_nirman_questionnaire') }}
)

select * from raw_questionnaire
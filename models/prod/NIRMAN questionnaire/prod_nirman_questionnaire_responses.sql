{{ config(materialized='table') }}

-- Join unpivoted data with question lookup to get actual question text
-- This creates a long format table with readable question text

with responses_with_questions as (
    select
        r.participant_id as response_id,
        r.participant_name,
        r.workshop_name,
        r.workshop_phase,
        r.question_code,
        q.category,
    q.question_text as question,
    -- prefer integer response when available, otherwise use textual response
    coalesce(cast(r.response_int as text), r.response_text) as response
    from {{ ref('int_nirman_questionnaire') }} r
    left join {{ ref('staging_nirman_questions_lookup') }} q
        on r.question_code = q.question_code
)

select
    response_id,
    participant_name as participant,
    workshop_name as workshop,
    workshop_phase,
    category,
    question_code,
    question,
    response
from responses_with_questions
order by 
    response_id, 
    case category
        when 'career' then 1
        when 'criteria' then 2
        when 'insecurities' then 3
        when 'social_contribution' then 4
        when 'questions' then 5
        when 'finance' then 6
    end,
    question_code
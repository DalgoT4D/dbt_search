{{ config(materialized='table') }}

-- Unpivot the questionnaire data using macro for cleaner code
with career_unpivoted as (
    select participant_id,
           participant_name,
           workshop_name,
           workshop_phase,
           batch,
           question_code,
           -- integer response when numeric
           case when trim(cast(response as text)) ~ '^\d+$' then cast(trim(cast(response as text)) as int) else null end as response_int,
           -- textual representation of response (kept for non-numeric answers)
           cast(response as text) as response_text
    from (
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'carrer', 1, 8) }}
    ) as u
),

criteria_unpivoted as (
    select participant_id,
           participant_name,
           workshop_name,
           workshop_phase,
           batch,
           question_code,
           case when trim(cast(response as text)) ~ '^\d+$' then cast(trim(cast(response as text)) as int) else null end as response_int,
           cast(response as text) as response_text
    from (
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'criteria', 1, 8) }}
    ) as u
),

insecurities_unpivoted as (
    select participant_id,
           participant_name,
           workshop_name,
           workshop_phase,
           batch,
           question_code,
           case when trim(cast(response as text)) ~ '^\d+$' then cast(trim(cast(response as text)) as int) else null end as response_int,
           cast(response as text) as response_text
    from (
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'insecurities', 1, 9) }}
    ) as u
),

social_contribution_unpivoted as (
    select 
        participant_id,
        participant_name,
        workshop_name,
        workshop_phase,
        batch,
    'social_contribution' as question_code,
    null::int as response_int,
    cast(social_contribution as text) as response_text
    from {{ ref('staging_nirman_questionnaire') }}
    where social_contribution is not null
),

questions_unpivoted as (
    select participant_id,
           participant_name,
           workshop_name,
           workshop_phase,
           batch,
           question_code,
           case when trim(cast(response as text)) ~ '^\d+$' then cast(trim(cast(response as text)) as int) else null end as response_int,
           cast(response as text) as response_text
    from (
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'question', 1, 41) }}
    ) as u
),

finance_unpivoted as (
    select participant_id,
           participant_name,
           workshop_name,
           workshop_phase,
           batch,
           question_code,
           case when trim(cast(response as text)) ~ '^\d+$' then cast(trim(cast(response as text)) as int) else null end as response_int,
           cast(response as text) as response_text
    from (
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'finance', 1, 4) }}
    ) as u
),

all_unpivoted as (
    select * from career_unpivoted
    union all
    select * from criteria_unpivoted
    union all
    select * from insecurities_unpivoted
    union all
    select * from social_contribution_unpivoted
    union all
    select * from questions_unpivoted
    union all
    select * from finance_unpivoted
)

select
    participant_id,
    participant_name,
    workshop_name,
    workshop_phase,
    batch,
    question_code,
    response_int,
    response_text
from all_unpivoted
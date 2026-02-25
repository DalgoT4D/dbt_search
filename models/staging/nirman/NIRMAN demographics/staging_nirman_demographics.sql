{{ config(materialized='table') }}

with source_data as (
    select
        id::text as participant_id,
        name::text as name,
        gender::text as gender,
        batch::text as batch,
        workshop_details::text as workshop,
        nirman_krutee_nirman::text as workshop_type,
        workshops_attended as workshops_attended,
        workshop_month_and_year::text as workshop_month_year,
        stream::text as educational_stream,
        state::text as native_state,
        district::text as native_district,
        tehsiltown::text as native_tehsiltown,
        point_2_workshop_name::text as point_2_workshop_name,
        point_2_workshop_invited::text as point_2_workshop_invited,
        point_2_workshop_attended::text as point_2_workshop_attended,
        point_3_workshop_name::text as point_3_workshop_name,
        point_3_workshop_invited::text as point_3_workshop_invited,
        point_3_workshop_attended::text as point_3_workshop_attended,
        case 
            when trim(level_of_education) = '' then null
            when upper(trim(level_of_education)) = 'NA' then null
            when upper(trim(level_of_education)) = 'N/A' then null
            when level_of_education is null then null
            else level_of_education::text
        end as level_of_education,
        current_status_of_work::text as current_status_of_work,
        case 
            when trim(person_years_of_social_action_as_on_31st_march_latest::text) = '' then null
            when upper(trim(person_years_of_social_action_as_on_31st_march_latest::text)) = 'NA' then null
            when upper(trim(person_years_of_social_action_as_on_31st_march_latest::text)) = 'N/A' then null
            when person_years_of_social_action_as_on_31st_march_latest is null then null
            else round(person_years_of_social_action_as_on_31st_march_latest::numeric, 1)
        end as person_years_social_action
    from {{ source('nirman', 'demographics_raw') }}
),

cleaned_data as (
    select
        participant_id,
        trim(name) as name,
        trim(upper(gender)) as gender,
        case
            when trim(batch) = '' then null
            when upper(trim(batch)) in ('NA', 'N/A') then null
            else nullif(regexp_replace(trim(batch), '[^0-9\\.]', '', 'g'), '')::numeric
        end as batch,
        trim(workshop) as workshop,
        trim(workshop_type) as workshop_type,
        trim(workshop_month_year) as workshop_month_year,
        workshops_attended,
        trim(educational_stream) as educational_stream_raw,
        trim(level_of_education) as level_of_education,
        trim(current_status_of_work) as current_status_of_work_raw,
        person_years_social_action,
        native_state,
        native_district,
        native_tehsiltown,
        point_2_workshop_name,
        point_2_workshop_invited,
        point_2_workshop_attended,
        point_3_workshop_name,
        point_3_workshop_invited,
        point_3_workshop_attended,
        
        -- Standardize Current Status of Work with full descriptions
        case
            when upper(trim(current_status_of_work)) = 'DT' 
                then 'Different Trajectory'
            when upper(trim(current_status_of_work)) = 'EDB' 
                then 'Educational Break'
            when upper(trim(current_status_of_work)) = 'OW' 
                then 'Other Work'
            when upper(trim(current_status_of_work)) = 'NOT NOW' 
                or lower(trim(current_status_of_work)) ilike '%not now%'
                then 'Previously SIW'
            when upper(trim(current_status_of_work)) = 'SIW' 
                or lower(trim(current_status_of_work)) ilike 'social impact work'
                then 'Social Impact Work'
            when upper(trim(current_status_of_work)) = 'NOT SURE'
                then 'Not Sure'
            else trim(current_status_of_work)
        end as current_status_of_work,
        
        -- Create Educational Stream 1 (categorized)
        case
            when lower(trim(educational_stream)) ilike '%medical%' 
                then 'Medical'    
            when lower(trim(educational_stream)) ilike '%engineering%'
                then 'Engineering'
            when lower(trim(educational_stream)) ilike 'arts%'
                or lower(trim(educational_stream)) ilike '%art%'
                or lower(trim(educational_stream)) ilike '%commerce%'
                or lower(trim(educational_stream)) ilike '%science%'
                or lower(trim(educational_stream)) ilike '%pharmancy%'
                then 'Arts/Commerce/Science/Pharmacy'
            else 'Other'
        end as educational_stream_1,
        
        -- Create Education_Status (Student vs Graduate)
        -- Priority 1: Check Current Status of Work first
        case
            -- Students: Only those explicitly marked as "Student"
            when lower(trim(current_status_of_work)) ilike '%student%'
                then 'Student'
            
            -- Graduates: SIW, Not Now, OW, EdB, DT
            when upper(trim(current_status_of_work)) = 'SIW'
                 or lower(trim(current_status_of_work)) ilike '%social impact work%'
                 or upper(trim(current_status_of_work)) = 'NOT NOW'
                 or lower(trim(current_status_of_work)) ilike '%not now%'
                 or lower(trim(current_status_of_work)) ilike '%prev siw%'
                 or upper(trim(current_status_of_work)) = 'OW'
                 or lower(trim(current_status_of_work)) ilike '%other work%'
                 or upper(trim(current_status_of_work)) = 'EDB'
                 or lower(trim(current_status_of_work)) ilike '%educational break%'
                 or upper(trim(current_status_of_work)) = 'DT'
                 or lower(trim(current_status_of_work)) ilike '%different trajectory%'
                then 'Graduate'
            
            -- Priority 2: If Current Status of Work is blank/null or "not sure", check Level of Education
            when (current_status_of_work is null 
                  or trim(current_status_of_work) = '' 
                  or lower(trim(current_status_of_work)) ilike '%not sure%')
                 and (
                     upper(trim(level_of_education)) = 'M'
                     or upper(trim(level_of_education)) = 'P'
                     or lower(trim(level_of_education)) ilike '%master%'
                     or lower(trim(level_of_education)) ilike '%phd%'
                 )
                then 'Graduate'
            
            -- Default: If still unclear, mark as Not Known
            else 'Not Known'
        end as education_status
        
    from source_data
)

select
    participant_id,
    name,
    gender,
    batch,
    workshop,
    workshop_type,
    workshop_month_year,
    workshops_attended,
    educational_stream_raw as educational_stream,
    educational_stream_1,
    current_status_of_work,
    person_years_social_action,
    education_status,
    native_state,
    native_district,
    native_tehsiltown,
    point_2_workshop_name,
    point_2_workshop_invited,
    point_2_workshop_attended,
    point_3_workshop_name,
    point_3_workshop_invited,
    point_3_workshop_attended
from cleaned_data
where participant_id is not null
order by participant_id
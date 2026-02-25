{{ config(materialized='table') }}

with demographics as (
    select * from {{ ref('staging_nirman_demographics') }}
),

final as (
    select
        participant_id as "Participant ID",
        name as "Name",
        gender as "Gender",
        batch as "Batch",
        workshop as "Workshop",
        workshop_type as "Workshop Type",
        workshop_month_year as "Workshop Month and Year",
        workshops_attended as "Workshops Attended",
        educational_stream as "Educational Stream",
        educational_stream_1 as "Educational Stream 1",
        current_status_of_work as "Current Status of Work",
        person_years_social_action as "Person Years of Social Action as on 31st Mar'25",
        education_status as "Education_Status",
        native_state as "Native State",
        native_district as "Native District",
        native_tehsiltown as "Native Tehsil/Town",
        point_2_workshop_name as "Point 2 Workshop Name",
        point_2_workshop_invited as "Point 2 Workshop Invited",
        point_2_workshop_attended as "Point 2 Workshop Attended",
        point_3_workshop_name as "Point 3 Workshop Name",
        point_3_workshop_invited as "Point 3 Workshop Invited",
        point_3_workshop_attended as "Point 3 Workshop Attended"
    from demographics
)

select * from final
order by "Participant ID"
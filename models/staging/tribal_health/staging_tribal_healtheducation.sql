{{ config(materialized='table') }}

with source_data as (
    select * 
    from {{ source('tribal_health', 'Health_Education') }}
),

cleaned as (
    select
        -- Date field (row 2)
        {{ validate_date('date') }} as date,
        
        -- Health education topics (row 3) 
        "select_the_health_education_topics_you_can_choose_more_than_one" as health_education_topics,
        
        -- Area (row 4)
        area,
        
        -- Villages by area (rows 5-9)
        villages_in_karwafa_area,
        villages_in_pendhari_area,
        villages_in_dhanora_area,
        villages_in_rangi_area,
        villages_in_murumgao_area,
        
        -- Session conducted (row 10)
        "was_health_education_session_conducted_" as session_conducted,
        
        -- Reason for not conducting (row 11)
        reason_for_not_conducting_session,
        
        -- Number of locations (row 12)
        "in_how_many_places_was_health_education_conducted_" as number_of_locations,
        
        -- Location 1 (rows 13-14)
        "location_1_name" as location_1_name,
        "location_1_number_of_participants" as location_1_participants,
        
        -- Location 2 (rows 15-16)
        "location_2_name" as location_2_name,
        "location_2_number_of_participants" as location_2_participants,
        
        -- Location 3 (rows 17-18)
        "location_3_name" as location_3_name,
        "location_3_number_of_participants" as location_3_participants,
        
        -- Location 4 (rows 19-20)
        "location_4_name" as location_4_name,
        "location_4_number_of_participants" as location_4_participants,
        
        -- Total participants (row 21)
        total_number_of_participants
        
        
    from source_data
)

select * from cleaned
{{ config(materialized='table') }}

with source_data as (
    select * 
    from {{ source('tribal_health', 'Health_Education') }}
),

cleaned as (
    select
        -- Date field
        {{ validate_date('date') }} as date,
        
        -- Health education topics - normalize known duplicates before storing
        -- मच्छरदाणी and मच्छरदाणीचा वापर are the same topic; standardize to मच्छरदाणीचा वापर
        replace(
            trim("select_the_health_education_topics_you_can_choose_more_than_one"),
            'मच्छरदाणी ',
            'मच्छरदाणीचा वापर '
        ) as health_education_topics,
        
        -- Area - trim whitespace to fix धानोरा double-counting
        trim(area) as area,
        
        -- Villages by area (separate columns, trim whitespace and nullify empty strings)
        nullif(trim(villages_in_karwafa_area), '')   as villages_in_karwafa_area,
        nullif(trim(villages_in_pendhari_area), '')  as villages_in_pendhari_area,
        nullif(trim(villages_in_dhanora_area), '')   as villages_in_dhanora_area,
        nullif(trim(villages_in_rangi_area), '')     as villages_in_rangi_area,
        nullif(trim(villages_in_murumgao_area), '')  as villages_in_murumgao_area,
        
        -- Session conducted
        "was_health_education_session_conducted_" as session_conducted,
        
        -- Reason for not conducting
        reason_for_not_conducting_session,
        
        -- Number of locations
        "in_how_many_places_was_health_education_conducted_" as number_of_locations,
        
        -- Location 1
        "location_1_name"                    as location_1_name,
        "location_1_number_of_participants"  as location_1_participants,
        
        -- Location 2
        "location_2_name"                    as location_2_name,
        "location_2_number_of_participants"  as location_2_participants,
        
        -- Location 3
        "location_3_name"                    as location_3_name,
        "location_3_number_of_participants"  as location_3_participants,
        
        -- Location 4
        "location_4_name"                    as location_4_name,
        "location_4_number_of_participants"  as location_4_participants,
        
        -- Total participants
        total_number_of_participants
        
    from source_data
)

select * from cleaned
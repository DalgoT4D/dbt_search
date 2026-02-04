{{ config(materialized='table') }}

-- Simple pass-through from staging, keeping all 277 rows intact
select 
    date,
    health_education_topics,
    area,
    
    -- Combine all village columns into single villages column
    concat_ws(', ', 
        nullif(villages_in_karwafa_area, ''),
        nullif(villages_in_pendhari_area, ''),
        nullif(villages_in_dhanora_area, ''),
        nullif(villages_in_rangi_area, ''),
        nullif(villages_in_murumgao_area, '')
    ) as villages,
    
    session_conducted,
    reason_for_not_conducting_session,
    number_of_locations,
    
    -- Locations with participants
    location_1_name,
    location_1_participants,
    location_2_name,
    location_2_participants,
    location_3_name,
    location_3_participants,
    location_4_name,
    location_4_participants,
    
    total_number_of_participants
    
from {{ ref('staging_tribal_healtheducation') }}
order by date desc
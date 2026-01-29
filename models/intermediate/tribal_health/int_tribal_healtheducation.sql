{{ config(materialized='table') }}

with base as (
    select * from {{ ref('staging_tribal_healtheducation') }}
),

-- Unpivot villages by area
villages_unpivoted as (
    select
        date,
        health_education_topics,
        area,
        session_conducted,
        reason_for_not_conducting_session,
        number_of_locations,
        total_number_of_participants,
        'Karwafa' as area_name,
        villages_in_karwafa_area as village_name
    from base
    where villages_in_karwafa_area is not null
    
    union all
    
    select
        date,
        health_education_topics,
        area,
        session_conducted,
        reason_for_not_conducting_session,
        number_of_locations,
        total_number_of_participants,
        'Pendhari' as area_name,
        villages_in_pendhari_area as village_name
    from base
    where villages_in_pendhari_area is not null
    
    union all
    
    select
        date,
        health_education_topics,
        area,
        session_conducted,
        reason_for_not_conducting_session,
        number_of_locations,
        total_number_of_participants,
        'Dhanora' as area_name,
        villages_in_dhanora_area as village_name
    from base
    where villages_in_dhanora_area is not null
    
    union all
    
    select
        date,
        health_education_topics,
        area,
        session_conducted,
        reason_for_not_conducting_session,
        number_of_locations,
        total_number_of_participants,
        'Rangi' as area_name,
        villages_in_rangi_area as village_name
    from base
    where villages_in_rangi_area is not null
    
    union all
    
    select
        date,
        health_education_topics,
        area,
        session_conducted,
        reason_for_not_conducting_session,
        number_of_locations,
        total_number_of_participants,
        'Murumgao' as area_name,
        villages_in_murumgao_area as village_name
    from base
    where villages_in_murumgao_area is not null
),

-- Unpivot locations with participants
locations_unpivoted as (
    select
        date,
        1 as location_sequence,
        location_1_name as location_name,
        location_1_participants as participants
    from base
    where location_1_name is not null
    
    union all
    
    select
        date,
        2 as location_sequence,
        location_2_name as location_name,
        location_2_participants as participants
    from base
    where location_2_name is not null
    
    union all
    
    select
        date,
        3 as location_sequence,
        location_3_name as location_name,
        location_3_participants as participants
    from base
    where location_3_name is not null
    
    union all
    
    select
        date,
        4 as location_sequence,
        location_4_name as location_name,
        location_4_participants as participants
    from base
    where location_4_name is not null
)

-- Join the unpivoted data back together
select
    v.date,
    v.health_education_topics,
    v.area,
    v.area_name,
    v.village_name,
    v.session_conducted,
    v.reason_for_not_conducting_session,
    v.number_of_locations,
    l.location_sequence,
    l.location_name,
    l.participants,
    v.total_number_of_participants
from villages_unpivoted v
left join locations_unpivoted l
    on v.date = l.date
order by v.date, v.area_name, l.location_sequence
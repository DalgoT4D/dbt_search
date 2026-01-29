{{ config(materialized='table') }}

with base as (
    select * from {{ ref('int_tribal_healtheducation') }}
),

final as (
    select
        date,
        health_education_topics,
        area,
        session_conducted,
        reason_for_not_conducting_session,
        
        -- Village information
        string_agg(distinct village_name, ', ' order by village_name) as villages_covered_list,
        count(distinct village_name) as villages_covered_count,
        count(distinct area_name) as areas_covered_count,
        
        -- Session-level metrics
        sum(participants::numeric) as total_participants,
        count(distinct location_name) as unique_locations,
        number_of_locations
        
    from base
    group by date, health_education_topics, area, session_conducted, 
             reason_for_not_conducting_session, number_of_locations
)

select * from final
order by date desc
{{ config(materialized='table') }}

-- ─────────────────────────────────────────────────────────────────────────────
-- One row per topic per session.
-- Use this model in Metabase to filter by "topic" as a plain text field.
-- 
-- Row count = sessions × topics covered per session
--   e.g. a session covering 3 topics → 3 rows, all other columns identical.
--
-- To add a new topic: only edit macros/health_education_topics.sql — done.
-- ─────────────────────────────────────────────────────────────────────────────

with base as (
    select * from {{ ref('prod_tribal_healtheducation') }}
),

topics_unpivoted as (

    {% for topic in get_health_education_topics() %}

    select
        date,
        area,
        villages,
        villages_in_karwafa_area,
        villages_in_pendhari_area,
        villages_in_dhanora_area,
        villages_in_rangi_area,
        villages_in_murumgao_area,
        session_conducted,
        reason_for_not_conducting_session,
        number_of_locations,
        location_1_name,
        location_1_participants,
        location_2_name,
        location_2_participants,
        location_3_name,
        location_3_participants,
        location_4_name,
        location_4_participants,
        total_number_of_participants,
        '{{ topic.label }}' as topic
    from base
    where {{ topic.column_name }} = true

    {% if not loop.last %} union all {% endif %}

    {% endfor %}

)

select * from topics_unpivoted
order by date desc, topic
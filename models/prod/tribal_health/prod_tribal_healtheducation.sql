{{ config(materialized='table') }}

-- ─────────────────────────────────────────────────────────────
-- Village name normalisation macro (inline)
-- Handles: trailing dots/spaces, known spelling variants,
--          suffix disambiguation (येनगाव 1 / येनगाव 2 etc.)
-- ─────────────────────────────────────────────────────────────

with staged as (
    select * from {{ ref('staging_tribal_healtheducation') }}
),

normalized as (
    select
        date,
        health_education_topics,

        -- ── Topic flags (true/false per row for Metabase filtering) ───
        -- Generated dynamically from macros/health_education_topics.sql
        -- To add / edit a topic: only touch that macro file, not this model.
        {{ topic_flags('health_education_topics') }},

        -- ── Area ──────────────────────────────────────────────
        -- trim() already applied in staging; no further change needed
        area,

        -- ── Villages – each area column kept separate ─────────
        -- Pattern applied to every village column:
        --   1. Strip trailing period / whitespace (सावंगा खुर्द. → सावंगा खुर्द)
        --   2. Collapse multiple internal spaces
        --   3. Map known spelling variants to canonical form

        {{ normalised_village_tribal('villages_in_karwafa_area') }}   as villages_in_karwafa_area,
        {{ normalised_village_tribal('villages_in_pendhari_area') }}  as villages_in_pendhari_area,
        {{ normalised_village_tribal('villages_in_dhanora_area') }}   as villages_in_dhanora_area,
        {{ normalised_village_tribal('villages_in_rangi_area') }}     as villages_in_rangi_area,
        {{ normalised_village_tribal('villages_in_murumgao_area') }}  as villages_in_murumgao_area,
        -- ── Combined village column ────────────────────────────
        concat_ws(', ',
            nullif({{ normalised_village_tribal('villages_in_karwafa_area') }}, ''),
            nullif({{ normalised_village_tribal('villages_in_pendhari_area') }}, ''),
            nullif({{ normalised_village_tribal('villages_in_dhanora_area') }}, ''),
            nullif({{ normalised_village_tribal('villages_in_rangi_area') }}, ''),
            nullif({{ normalised_village_tribal('villages_in_murumgao_area') }}, '')
        ) as villages,

        session_conducted,
        reason_for_not_conducting_session,
        cast(number_of_locations       as integer) as number_of_locations,

        location_1_name,
        cast(location_1_participants   as integer) as location_1_participants,
        location_2_name,
        cast(location_2_participants   as integer) as location_2_participants,
        location_3_name,
        cast(location_3_participants   as integer) as location_3_participants,
        location_4_name,
        cast(location_4_participants   as integer) as location_4_participants,

        cast(total_number_of_participants as integer) as total_number_of_participants

    from staged
)

select * from normalized
order by date desc
{{ config(materialized='table') }}

with mmu1_data as (
    select * from {{ ref('staging_mmu_1') }}
),

mmu2_data as (
    select * from {{ ref('staging_mmu_2') }}
),

combined as (
    select * from mmu1_data
    union all
    select * from mmu2_data
),

deduped as (
    select 
        *,
        row_number() over (
            partition by patient_unique_id, visit_date, serial_no, source_system
            order by dbt_load_timestamp desc
        ) as rn
    from combined
),

final as (
    select
        -- Remove row number and audit fields for cleaner dataset
        serial_no,
        patient_unique_id,
        patient_name,
        gender,
        age_years,
        age_group,
        area,
        village,
        diagnosis1,
        diagnosis2,
        diagnosis3,
        diagnosis4,
        diagnosis5,
        is_referred,
        is_newly_diagnosed,
        is_followup,
        visit_date,
        'Week' || COALESCE(extract(week from visit_date)::text, 'Unknown') as visit_week,
        submitted_by,
        doctor_name,
        source_system,
        COALESCE(to_char(visit_date, 'Month'), 'Unknown') as visit_month,
        COALESCE(extract(year from visit_date)::text, 'Unknown') as visit_year
    from deduped
    where rn = 1  -- Keep first record if there are still exact duplicates
)

select * from final 
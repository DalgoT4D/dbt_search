{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging_tribal_health_mmu_data', 'MMU_1') }}
),

cleaned as (
    select
        -- Primary keys
        serial_no,
        patient_unique_id,
        
        -- Patient demographics
        trim(name_of_the_patient) as patient_name,
        lower(trim(sex)) as gender,
        
        -- Age standardization
        coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) as age_years,
        case 
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) < 1 then '0-1'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) between 1 and 5 then '1-5'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) between 6 and 12 then '6-12'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) between 13 and 17 then '13-17'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) between 18 and 30 then '18-30'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) between 31 and 45 then '31-45'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) between 46 and 60 then '46-60'
            when coalesce(cast(nullif(t_age,'') as numeric)::integer, cast(nullif(age_years,'') as numeric)::integer, 0) > 60 then '60+'
            else 'Unknown'
        end as age_group,
        
        -- Location
        trim(area) as area,
        trim(name_of_the_village) as village,
        
        -- Diagnoses (clean and standardize)
        case 
            when trim(diagnosis_1) in ('Blank', '') then null 
            else trim(diagnosis_1) 
        end as diagnosis1,
        case 
            when trim(diagnosis_2) in ('Blank', '') then null 
            else trim(diagnosis_2) 
        end as diagnosis2,
        case 
            when trim(diagnosis_3) in ('Blank', '') then null 
            else trim(diagnosis_3) 
        end as diagnosis3,
        case 
            when trim(diagnosis_4) in ('Blank', '') then null 
            else trim(diagnosis_4) 
        end as diagnosis4,
        case 
            when trim(p_diagnosis_5) in ('Blank', '') then null 
            else trim(p_diagnosis_5) 
        end as diagnosis5,
        
        -- Visit type flags
        case when lower(p_refered) = 'yes' then true else false end as is_referred,
        case when lower(p_newly_diag) = 'yes' then true else false end as is_newly_diagnosed,
        case when lower(p_followup) = 'yes' then true else false end as is_followup,
        
        -- Date construction and validation
        {{ validate_date("concat(yy, '-', lpad(mm::text, 2, '0'), '-', lpad(dd::text, 2, '0'))") }} as visit_date,
        
        -- Extract week and month for dashboard filtering
        date_trunc('week', {{ validate_date("concat(yy, '-', lpad(mm::text, 2, '0'), '-', lpad(dd::text, 2, '0'))") }}) as visit_week,
        date_trunc('month', {{ validate_date("concat(yy, '-', lpad(mm::text, 2, '0'), '-', lpad(dd::text, 2, '0'))") }}) as visit_month,
        extract(year from {{ validate_date("concat(yy, '-', lpad(mm::text, 2, '0'), '-', lpad(dd::text, 2, '0'))") }}) as visit_year,

        -- Staff information
        trim(submitted_by) as submitted_by,
        trim(dr_names) as doctor_name,
        
        -- Source identifier
        'MMU1' as source_system,
        
        -- Audit fields
        current_timestamp as dbt_load_timestamp
        
    from source
    where patient_unique_id is not null
      and trim(patient_unique_id) != ''
)

select * from cleaned
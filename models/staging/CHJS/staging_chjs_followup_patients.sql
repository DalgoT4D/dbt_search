{{ config(materialized='table') }}

-- Staging model for CHJS Follow-up Patient data
-- This model reads from the source table and standardizes column names

with source_data as (
    select
        -- Patient identification
        registeration_no_ as registration_no,
        name as patient_name,
        age,
        gender as gender_code,
        
        -- Date fields
        day,
        month,
        year,
        -- Construct visit_date when day/month/year are present
        case
            when year is not null and month is not null and day is not null then
                make_date(
                    cast(year as integer),
                    cast(month as integer),
                    cast(day as integer)
                )
            else null
        end as visit_date,
        
        -- Coded categorical fields
        type_ as type_code,
        -- derive human readable label for type_code here so outer select can reuse it
        case
            when trim(type_) in ('1') then 'REGULAR (OPD)'
            when trim(type_) in ('2') then 'TELE'
            when trim(type_) in ('3') then 'SPECIALIST'
            else null
        end as type_label,
        speciality_ as speciality_code,
        area_affected_ as area_affected_code,
        
        -- Diagnosis and advice codes
        n_diagnosis_1 as n_diagnosis_1_code,
        n_advice_1_ as n_advice_1_code,
        
        -- Red flag indicators
        red_flag_ as red_flag_code,
        -- Normalize red flag description: trim, collapse spaces, uppercase; expand common abbreviations
        case
            when if_red_flag_yes_then_mention_red_flag is null then null
            when trim(if_red_flag_yes_then_mention_red_flag) = '' then null
            when lower(trim(if_red_flag_yes_then_mention_red_flag)) in ('lbp') then 'LOW BACK PAIN'
            when lower(trim(if_red_flag_yes_then_mention_red_flag)) in ('myalagia', 'myalgia') then 'MYALGIA'
            else upper(regexp_replace(trim(if_red_flag_yes_then_mention_red_flag), '\s+', ' ', 'g'))
        end as red_flag_description,
        
        -- Imaging fields
        mri_suggested as mri_suggested_code,
        mri_referred as mri_referred_code,
        patient_return_with_mri as patient_return_with_mri_code,
        
        -- Other diagnosis codes (free text)
        other_daignosis,
        
        -- Patient type indicator
        'Follow-up' as patient_type
        
    from {{ source('chjs_raw', 'followup_patients') }}
    where registeration_no_ is not null
)

select
    registration_no,
    patient_name,
    age,
    gender_code,
    
    -- Use precomputed visit_date from source_data
    visit_date,
    -- derived month name and year
    COALESCE(to_char(visit_date, 'FMMonth'), 'Unknown') as visit_month,
    COALESCE(to_char(visit_date, 'YYYY'), 'Unknown') as visit_year,

    speciality_code,
    area_affected_code,
    -- OPD session: formatted as DD/MM/YYYY-Type (e.g., 26/09/2025-REGULAR (OPD))
    CASE
        WHEN visit_date IS NOT NULL THEN to_char(visit_date, 'DD/MM/YYYY') || '-' || COALESCE(type_label, 'Unknown')
        ELSE COALESCE(type_label, 'Unknown')
    END as opd_session,
    type_label,
    
    n_diagnosis_1_code,
    n_advice_1_code,
    
    red_flag_code,
    red_flag_description,
    
    mri_suggested_code,
    mri_referred_code,
    patient_return_with_mri_code,
    
    -- Normalize free-text other diagnosis fields
    case
        when other_daignosis is null then null
        when trim(other_daignosis) = '' then null
        when lower(trim(other_daignosis)) in ('lbp') then 'LOW BACK PAIN'
        when lower(trim(other_daignosis)) in ('myalagia', 'myalgia') then 'MYALGIA'
        else upper(regexp_replace(trim(other_daignosis), '\s+', ' ', 'g'))
    end as other_daignosis,
    
    patient_type
    
from source_data
where visit_date <= current_date -- Filter out future dates
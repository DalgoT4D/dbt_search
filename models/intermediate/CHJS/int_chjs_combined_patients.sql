{{ config(materialized='table') }}

-- Intermediate model to combine new and follow-up patients
-- This unions both datasets with standardized structure

with new_patients as (
    select
        registration_no,
        patient_name,
        age,
        gender_code,
        visit_date,
        visit_month,
        visit_year,
        patient_type,
        -- alias the human-readable type label to the expected column name
        type_label as type,
        speciality_code,
        area_affected_code,
        n_diagnosis_1_code,
        n_advice_1_code,
        n_diagnosis_2_code,
        n_advice_2_code,
        n_diagnosis_3_code,
        n_advice_3_code,
        red_flag_code,
        red_flag_description,
        mri_suggested_code,
        mri_referred_code,
        xray_mri_done_code,
        other_diagnosis_1,
        other_diagnosis_2,
        other_diagnosis_3,
        null as other_daignosis,
        null as patient_return_with_mri_code,
        opd_session
    from {{ ref('staging_chjs_new_patients') }}
),

followup_patients as (
    select
        registration_no,
        patient_name,
        age,
        gender_code,
        visit_date,
        visit_month,
        visit_year,
        patient_type,
        -- alias type_label to the same output column name
        type_label as type,
        speciality_code,
        area_affected_code,
        n_diagnosis_1_code,
        n_advice_1_code,
        null as n_diagnosis_2_code,
        null as n_advice_2_code,
        null as n_diagnosis_3_code,
        null as n_advice_3_code,
        red_flag_code,
        red_flag_description,
        mri_suggested_code,
        mri_referred_code,
        null as xray_mri_done_code,
        null as other_diagnosis_1,
        null as other_diagnosis_2,
        null as other_diagnosis_3,
        other_daignosis,
        patient_return_with_mri_code,
        opd_session
    from {{ ref('staging_chjs_followup_patients') }}
),

combined as (
    select * from new_patients
    union all
    select * from followup_patients
)

select
    -- Patient identification
    registration_no,
    patient_name,
    age,
    gender_code,
    visit_date,
    visit_month,
    visit_year,
    
    patient_type,
    
    -- Coded fields
    type,
    speciality_code,
    area_affected_code,
    
    -- Diagnosis and advice
    n_diagnosis_1_code,
    n_advice_1_code,
    n_diagnosis_2_code,
    n_advice_2_code,
    n_diagnosis_3_code,
    n_advice_3_code,
    
    -- Red flags
    red_flag_code,
    red_flag_description,
    
    -- Imaging
    mri_suggested_code,
    mri_referred_code,
    patient_return_with_mri_code,
    xray_mri_done_code,
    
    -- Other diagnoses (already text descriptions)
    other_diagnosis_1,
    other_diagnosis_2,
    other_diagnosis_3,
    other_daignosis,
    opd_session
    
from combined
{{ config(materialized='table') }}

-- Production model with decoded values using the seed codes
-- This joins with the seed code table to replace codes with human-readable descriptions

with patients as (
    select * from {{ ref('int_chjs_combined_patients') }}
),

codes as (
    select * from {{ ref('seeds_chjs_codes') }}
)

select
    -- Patient identification
    p.registration_no,
    p.patient_name,
    p.age,
    coalesce(g.description, cast(p.gender_code as text)) as gender,
    p.visit_date,
    p.visit_month,
    p.visit_year,
    p.patient_type,
    p.type,
    
    -- Decoded categorical fields
    coalesce(s.description, cast(p.speciality_code as text)) as speciality,
    coalesce(aa.description, cast(p.area_affected_code as text)) as area_affected,
    
    -- Decoded diagnosis and advice (using Diagnosis code type)
    coalesce(nd1.description, cast(p.n_diagnosis_1_code as text)) as n_diagnosis_1,
    coalesce(na1.description, cast(p.n_advice_1_code as text)) as n_advice_1,
    coalesce(nd2.description, cast(p.n_diagnosis_2_code as text)) as n_diagnosis_2,
    coalesce(na2.description, cast(p.n_advice_2_code as text)) as n_advice_2,
    coalesce(nd3.description, cast(p.n_diagnosis_3_code as text)) as n_diagnosis_3,
    coalesce(na3.description, cast(p.n_advice_3_code as text)) as n_advice_3,
    
    -- Red flag handling
    coalesce(rf.description, 
        case 
            when p.red_flag_code = '1' then 'Yes'
            when p.red_flag_code = '2' then 'No'
            else cast(p.red_flag_code as text)
        end
    ) as red_flag,
    p.red_flag_description,
    
    -- Decoded imaging fields
    coalesce(ms.description,
        case 
            when p.mri_suggested_code = '1' then 'Yes'
            when p.mri_suggested_code = '2' then 'No'
            else cast(p.mri_suggested_code as text)
        end
    ) as mri_suggested,
    
    coalesce(mr.description, cast(p.mri_referred_code as text)) as mri_referred,
    
    coalesce(xm.description,
        case 
            when p.xray_mri_done_code = '1' then 'Yes'
            when p.xray_mri_done_code = '2' then 'No'
            else cast(p.xray_mri_done_code as text)
        end
    ) as xray_mri_already_done,

    coalesce(prm.description,
        case 
            when p.patient_return_with_mri_code = '1' then 'Yes'
            when p.patient_return_with_mri_code = '2' then 'No'
            else cast(p.patient_return_with_mri_code as text)
        end
    ) as patient_return_with_mri,
    
    -- Other diagnoses (already text)
    p.other_diagnosis_1,
    p.other_diagnosis_2,
    p.other_diagnosis_3,
    p.other_daignosis,
    p.opd_session
    
from patients p

-- Join with code mappings for each coded field
left join codes g 
    on g.code_type = 'Gender' 
    and cast(p.gender_code as text) = g.code
    
left join codes s 
    on s.code_type = 'Speciality' 
    and cast(p.speciality_code as text) = s.code
    
left join codes aa 
    on aa.code_type = 'Area_Affected' 
    and cast(p.area_affected_code as text) = aa.code
    
left join codes nd1 
    on nd1.code_type = 'Diagnosis' 
    and cast(p.n_diagnosis_1_code as text) = nd1.code
    
left join codes na1 
    on na1.code_type = 'Advice' 
    and cast(p.n_advice_1_code as text) = na1.code
    
left join codes nd2 
    on nd2.code_type = 'Diagnosis' 
    and cast(p.n_diagnosis_2_code as text) = nd2.code
    
left join codes na2 
    on na2.code_type = 'Advice' 
    and cast(p.n_advice_2_code as text) = na2.code
    
left join codes nd3 
    on nd3.code_type = 'Diagnosis' 
    and cast(p.n_diagnosis_3_code as text) = nd3.code
    
left join codes na3 
    on na3.code_type = 'Advice' 
    and cast(p.n_advice_3_code as text) = na3.code
    
left join codes rf 
    on rf.code_type = 'Yes_No' 
    and cast(p.red_flag_code as text) = rf.code
    
left join codes ms 
    on ms.code_type = 'Yes_No' 
    and cast(p.mri_suggested_code as text) = ms.code
    
left join codes mr 
    on mr.code_type = 'MRI_Referred' 
    and cast(p.mri_referred_code as text) = mr.code
    
left join codes xm 
    on xm.code_type = 'Yes_No' 
    and cast(p.xray_mri_done_code as text) = xm.code

left join codes prm
    on prm.code_type = 'Yes_No'
    and cast(p.patient_return_with_mri_code as text) = prm.code
{{ config(materialized='table') }}

-- Staging lookup table with all CHJS code mappings
-- This creates a comprehensive reference table for all coded fields
-- Alternative to using a CSV seed file - codes are defined directly in SQL

with diagnosis_codes as (
    select 'Diagnosis' as code_type, '1' as code, 'NON SPECIFIC' as description union all
    select 'Diagnosis', '2', 'FROZEN' union all
    select 'Diagnosis', '3', 'TEAR' union all
    select 'Diagnosis', '4', 'FRACTURE' union all
    select 'Diagnosis', '5', 'DISLOCATION' union all
    select 'Diagnosis', '6', 'TENDINITIS' union all
    select 'Diagnosis', '7', 'BURSITIS' union all
    select 'Diagnosis', '8', 'SPRAIN/STRAIN' union all
    select 'Diagnosis', '9', 'TENNIS ELBOW' union all
    select 'Diagnosis', '10', 'MEDIAL EPICONDILITIS' union all
    select 'Diagnosis', '11', 'CUBITAL TUNNEL SYNDROME' union all
    select 'Diagnosis', '12', 'LIGAMENT INJURY' union all
    select 'Diagnosis', '13', 'ULNAR NERVE ENTRAPMENT' union all
    select 'Diagnosis', '14', 'FRACTURE / TRAUMA' union all
    select 'Diagnosis', '15', 'OLECRANON BURSITIS' union all
    select 'Diagnosis', '16', 'CARPAL TUNNEL SYNDROME' union all
    select 'Diagnosis', '17', 'ARTHRITIS' union all
    select 'Diagnosis', '18', 'TRIGGER FINGER' union all
    select 'Diagnosis', '19', 'DEQUIRVEIN TENOSYNOVITIS' union all
    select 'Diagnosis', '20', 'OSTEOARTHERITIS' union all
    select 'Diagnosis', '21', 'AVN HIP' union all
    select 'Diagnosis', '22', 'PERTHE''S' union all
    select 'Diagnosis', '23', 'IRRITATION HIP' union all
    select 'Diagnosis', '24', 'SEPTIC ARTHERITIS' union all
    select 'Diagnosis', '25', 'MENISCUS TEAR' union all
    select 'Diagnosis', '26', 'BURSITIS/TENDINITIS' union all
    select 'Diagnosis', '27', 'LIGAMENT/CARTILAGE INJURY' union all
    select 'Diagnosis', '28', 'ANKLE SPRAIN' union all
    select 'Diagnosis', '29', 'PLANTER FASCITIS' union all
    select 'Diagnosis', '30', 'OA ANKLE' union all
    select 'Diagnosis', '31', 'ANKLE JOINT INSTABILITY' union all
    select 'Diagnosis', '32', 'ACHILIS TENDINOPATHY' union all
    select 'Diagnosis', '33', 'FOOT DROP' union all
    select 'Diagnosis', '34', 'STENOSIS' union all
    select 'Diagnosis', '35', 'PIVD' union all
    select 'Diagnosis', '36', 'SPONDYLOLISTHESIS' union all
    select 'Diagnosis', '37', 'ROOT COMPRESSION' union all
    select 'Diagnosis', '38', 'TUMOURS' union all
    select 'Diagnosis', '39', 'MYELOPATHY' union all
    select 'Diagnosis', '40', 'TB SPONDYLODISCITIS' union all
    select 'Diagnosis', '41', 'NON TB SPONDYLODISCITIS' union all
    select 'Diagnosis', '42', 'TRAUMATIC FRACTURE' union all
    select 'Diagnosis', '43', 'OSTEOPOROTIC FRACTURES' union all
    select 'Diagnosis', '44', 'PATHOLOGICAL FRACTURES' union all
    select 'Diagnosis', '45', 'DEGENERATIVE DISC DISEASE' union all
    select 'Diagnosis', '46', 'NON SPECIFIC NECK PAIN' union all
    select 'Diagnosis', '47', 'NON SPECIFIC LOW BACK PAIN' union all
    select 'Diagnosis', '48', 'OPERATIVE CASES' union all
    select 'Diagnosis', '49', 'SPONDYLOSIS' union all
    select 'Diagnosis', '50', 'KYPHOSIS' union all
    select 'Diagnosis', '51', 'FIBROFACITIS' union all
    select 'Diagnosis', '52', 'SCOLIOSIS' union all
    select 'Diagnosis', '53', 'NON SPECIFIC UPPER BACK PAIN' union all
    select 'Diagnosis', '54', 'SI JOINT PAIN' union all
    select 'Diagnosis', '55', 'ANKYLOSING SPONDYLOSIS' union all
    select 'Diagnosis', '56', 'RHEUMATOID ARTHRITIS' union all
    select 'Diagnosis', '57', 'GOUT' union all
    select 'Diagnosis', '58', 'INFECTIOUS ARTHERITIS' union all
    select 'Diagnosis', '59', 'PSOARIATIC INFLAMMATORY ARTHRITIS' union all
    select 'Diagnosis', '60', 'POLYARTHERITIS' union all
    select 'Diagnosis', '61', 'CANCER' union all
    select 'Diagnosis', '62', 'TMJ DISORDER' union all
    select 'Diagnosis', '63', 'CHRONIC FATIGUE SYNDROME' union all
    select 'Diagnosis', '64', 'RESISTANT PAIN DESPITE TREATMENT' union all
    select 'Diagnosis', '65', 'POST HERPETC NEURALGIA' union all
    select 'Diagnosis', '66', 'TRIGEMINAL NEURALGIA' union all
    select 'Diagnosis', '67', 'CRPS / RSD type 1' union all
    select 'Diagnosis', '68', 'DIABETIC NEUROPATHIC' union all
    select 'Diagnosis', '69', 'POST SURGERY NERVE ENTRAPMENT' union all
    select 'Diagnosis', '70', 'OTHER NERVE PAIN' union all
    select 'Diagnosis', '71', 'HARNIA / POST SPINE SURGERY / POST THORACOTMIES' union all
    select 'Diagnosis', '72', 'POST SURGERY MYOFACIAL PAIN' union all
    select 'Diagnosis', '73', 'FACET JOINT SYNDROME' union all
    select 'Diagnosis', '74', 'POST SPINE SURGERY PAIN' union all
    select 'Diagnosis', '75', 'VERTEBRAL BODY FRACTURE/METASTASIS' union all
    select 'Diagnosis', '76', 'DISCOGENIC DISC DISEASE' union all
    select 'Diagnosis', '77', 'TRAPEZIUS' union all
    select 'Diagnosis', '78', 'GLUTEAL' union all
    select 'Diagnosis', '79', 'PIRIFORMIS' union all
    select 'Diagnosis', '80', 'QUADRATURE LUMBORUM' union all
    select 'Diagnosis', '81', 'QUADRICEPS' union all
    select 'Diagnosis', '82', 'LEVATOR SCAPULAE' union all
    select 'Diagnosis', '83', 'RHOMBOID' union all
    select 'Diagnosis', '84', 'MIGRAINE' union all
    select 'Diagnosis', '85', 'TENSION TYPE' union all
    select 'Diagnosis', '86', 'CLUSTER' union all
    select 'Diagnosis', '87', 'CERVICOGENIC' union all
    select 'Diagnosis', '88', 'MEDICINE OVERUSE' union all
    select 'Diagnosis', '89', 'PSYCOGENIC' union all
    select 'Diagnosis', '90', 'KNEE' union all
    select 'Diagnosis', '91', 'SHOULDER' union all
    select 'Diagnosis', '92', 'ANKLE' union all
    select 'Diagnosis', '93', 'HIP' union all
    select 'Diagnosis', '94', 'OTHERS'
),

advice_codes as (
    select 'Advice' as code_type, '1' as code, 'PHYSIOTHERAPY' as description union all
    select 'Advice', '2', 'CONSERVATIVE' union all
    select 'Advice', '3', 'SURGERY' union all
    select 'Advice', '4', 'BLOCK' union all
    select 'Advice', '5', 'MEDICATION' union all
    select 'Advice', '6', 'OTHERS'
),

mri_referred_codes as (
    select 'MRI_Referred' as code_type, '1' as code, 'BRAHMPURI HOSPITAL' as description union all
    select 'MRI_Referred', '2', 'DR. AJAY MEHRA' union all
    select 'MRI_Referred', '3', 'DR. ANIL MADURWA CHANDERPUR' union all
    select 'MRI_Referred', '4', 'STAR IMAGING' union all
    select 'MRI_Referred', '5', 'OTHERS' union all
    select 'MRI_Referred', '6', 'NOT REFERRED'
),

area_affected_codes as (
    select 'Area_Affected' as code_type, '1' as code, 'SHOULDER' as description union all
    select 'Area_Affected', '2', 'ELBOW' union all
    select 'Area_Affected', '3', 'WRIST' union all
    select 'Area_Affected', '4', 'HIP' union all
    select 'Area_Affected', '5', 'KNEE' union all
    select 'Area_Affected', '6', 'ANKLE' union all
    select 'Area_Affected', '7', 'CERVICAL' union all
    select 'Area_Affected', '8', 'THORACIC' union all
    select 'Area_Affected', '9', 'LUMBAR' union all
    select 'Area_Affected', '10', 'ANKYLOSING SPONDYLOSIS' union all
    select 'Area_Affected', '11', 'RHEUMATOID ARTHRITIS' union all
    select 'Area_Affected', '12', 'OSTEOARTHERITIS' union all
    select 'Area_Affected', '13', 'GOUT' union all
    select 'Area_Affected', '14', 'INFECTIOUS_ARTHERITIS' union all
    select 'Area_Affected', '15', 'PSOARIATIC INFLAMMATORY ARTHRITIS' union all
    select 'Area_Affected', '16', 'POLYARTHERITIS' union all
    select 'Area_Affected', '17', 'SPINE_D' union all
    select 'Area_Affected', '18', 'MYOFASCIAL_PAIN' union all
    select 'Area_Affected', '19', 'HEADACHE' union all
    select 'Area_Affected', '20', 'JOINT_PAIN' union all
    select 'Area_Affected', '21', 'NEUROPATHIC_PAIN' union all
    select 'Area_Affected', '22', 'POST_SURGERY_PAIN' union all
    select 'Area_Affected', '23', 'OTHERS'
),

speciality_codes as (
    select 'Speciality' as code_type, '1' as code, 'ORTHOPEDICS' as description union all
    select 'Speciality', '2', 'SPINE' union all
    select 'Speciality', '3', 'RHEUMATOLOGY' union all
    select 'Speciality', '4', 'PAIN'
),

gender_codes as (
    select 'Gender' as code_type, '1' as code, 'Male' as description union all
    select 'Gender', '2', 'Female'
),

yes_no_codes as (
    select 'Yes_No' as code_type, '1' as code, 'Yes' as description union all
    select 'Yes_No', '2', 'No'
)

-- Combine all code types
select * from diagnosis_codes
union all
select * from advice_codes
union all
select * from mri_referred_codes
union all
select * from area_affected_codes
union all
select * from speciality_codes
union all
select * from gender_codes
union all
select * from yes_no_codes
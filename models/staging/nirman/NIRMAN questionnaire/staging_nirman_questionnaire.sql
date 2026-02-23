{{ config(materialized='table') }}

-- Stage 1: Extract raw questionnaire data with selected columns
with raw_questionnaire as (
    select
        participant_id,
        participant_name,
        workshop_name,
        workshop_phase,
        batch,
        
        -- Career columns (note: source has 'carrer' typo)
        carrer_1,
        carrer_2,
        carrer_3,
        carrer_4,
        carrer_5,
        carrer_6,
        carrer_7,
        carrer_8,
        
        -- Criteria columns
        criteria_1,
        criteria_2,
        criteria_3,
        criteria_4,
        criteria_5,
        criteria_6,
        criteria_7,
        criteria_8,
        
        -- Insecurities columns
        insecurities_1,
        insecurities_2,
        insecurities_3,
        insecurities_4,
        insecurities_5,
        insecurities_6,
        insecurities_7,
        insecurities_8,
        insecurities_9,
        insecurities_10,
        
        -- Social contribution
        social_contribution,
        
        -- Question columns (1-41)
        question_1,
        question_2,
        question_3,
        question_4,
        question_5,
        question_6,
        question_7,
        question_8,
        question_9,
        question_10,
        question_11,
        question_12,
        question_13,
        question_14,
        question_15,
        question_16,
        question_17,
        question_18,
        question_19,
        question_20,
        question_21,
        question_22,
        question_23,
        question_24,
        question_25,
        question_26,
        question_27,
        question_28,
        question_29,
        question_30,
        question_31,
        question_32,
        question_33,
        question_34,
        question_35,
        question_36,
        question_37,
        question_38,
        question_39,
        question_40,
        question_41,
        
        -- Finance columns
        finance_1,
        finance_2,
        finance_3,
        finance_4
        
    from {{ source('staging_nirman_questionnaire', 'staging_nirman_questionnaire') }}
)

select * from raw_questionnaire
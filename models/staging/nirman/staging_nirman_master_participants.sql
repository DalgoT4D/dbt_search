{{ config(materialized='table',
tags=["nirman"]) }}

with ranked_participants as (
    select
        *,
        -- Create a normalized name for joining (lowercase, underscores, no spaces)
        lower(replace(name, ' ', '_')) as name_normalized,
        -- Rank by most complete record (fewer nulls) and most recent data
        row_number() over (
            partition by lower(replace(name, ' ', '_')), dob
            order by 
                -- Prefer records with more complete data (fewer null fields)
                case when main_batch is not null then 0 else 1 end,
                case when current_age is not null then 0 else 1 end,
                case when educational_degree is not null then 0 else 1 end,
                case when current_domain_of_work is not null then 0 else 1 end,
                -- Use ID as tiebreaker (lexicographically)
                id desc
        ) as rn
    from {{ source('staging_nirman_master_data', 'master_database_participants') }}
    where id is not null
)

select
    id,
    dob,
    name,
    name_normalized,
    gender,
    mobile,
    sr_no_,
    email_id,
    sub_batch,
    main_batch,
    speciality,
    current_age,
    native_area,
    nirman_kr_n,
    current_area,
    native_state,
    alternate_no_,
    current_state,
    native_tehsil,
    current_partner,
    native_district,
    current_district,
    follow_up_person,
    worked_in_search,
    _0_2_invited_y_n_,
    follow_up_2023_24,
    educational_degree,
    educational_stream,
    workshops_attended,
    mode_of_application,
    worked_in_gadchiroli,
    couple_pairs_in_nirman,
    current_domain_of_work,
    current_status_of_work,
    sibling_pairs_in_nirman,
    medical_doctors_category,
    age_when_applied_to_nirman,
    current_level_of_education,
    potential_for_10_3_workshop,
    _0_1_attended_workshop_name_,
    _0_2_attended_workshop_name_,
    _0_3_attended_workshop_name_,
    financial_contribution_status,
    working_with_the_organization,
    linked_in_joined_to_nirman_page,
    slide_for_website_ppt_instagram,
    last_date_of_contact_established,
    potential_for_social_contribution,
    started_siw_in_the_year_2022_2023,
    started_siw_in_the_year_2023_2024,
    started_siw_in_the_year_2024_2025,
    worked_as_an_mo_in_public_health_system,
    financial_model_of_support_only_for_siw_,
    people_to_contact_for_database_completion,
    those_who_continue_beyond_5_years_as_a_siw,
    person_years_of_social_action_as_on_31_st_mar_21,
    person_years_of_social_action_as_on_31_st_mar_22,
    person_years_of_social_action_as_on_31_st_mar_23,
    person_years_of_social_action_as_on_31_st_mar_24,
    person_years_of_social_action_as_on_31_st_mar_25,
    status_of_net_being_in_touch_as_on_31_st_mar_of_the_year,
    _0_3_invited_same_batch_invited_later_consider_later_no_na_,
    potential_net_member_or_fellow_or_intern_or_volunteer_or_rp,
    type_of_organization_where_s_he_had_worked_in_the_social_sector

from ranked_participants
where rn = 1
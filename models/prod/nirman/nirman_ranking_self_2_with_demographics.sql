{{ config(materialized='table',
tags=["nirman"]) }}

select
    self2.*,
    demo.id as participant_id,
    demo.gender,
    demo.current_age,
    demo.native_area,
    demo.current_area,
    demo.native_state,
    demo.current_state,
    demo.native_district,
    demo.current_district,
    demo.educational_degree,
    demo.educational_stream,
    demo.speciality,
    demo.main_batch,
    demo.sub_batch,
    demo.workshops_attended,
    demo.current_domain_of_work,
    demo.current_status_of_work

from {{ ref('nirman_ranking_self_2') }} self2
left join {{ ref('staging_nirman_master_participants') }} demo
    on lower(replace(self2.name, ' ', '_')) = demo.name_normalized
{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging_village_master_data', 'Matched Villages') }}
),

cleaned as (
    select
        trim(marathi) as marathi,
        trim(english) as english,
        trim(district) as district,
        trim(taluka) as taluka
    from source
    where marathi is not null
      and english is not null
      and district is not null
      and taluka is not null
      and marathi != ''
      and english != ''
      and district != ''
      and taluka != ''
)

select
    marathi,
    english,
    district,
    taluka
from cleaned
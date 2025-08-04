{{ config(materialized='table') }}

select
    _id,
    case 
        when "end" is null then null
        when "end" ~ '^\d{1,2}/\d{1,2}/\d{4}$' then "end"
        else null
    end as "end",
    "Area",
    case 
        when "Date" is null then null
        when "Date" ~ '^\d{1,2}/\d{1,2}/\d{4}$' then "Date"
        else null
    end as "Date",
    _tags,
    _uuid,
    start,
    _index,
    _notes,
    _status,
    __version__,
    "Village_Name",
    "Village_GPS_",
    "Place_4_Photo",
    _submitted_by,
    "Place_1___Name",
    "Place_2___Name",
    "Place_3___Name",
    "Place_4___Name",
    "Place_1___Photo",
    "Place_2___Photo",
    "Place_3___Photo",
    case 
        when "Total_Attendance" is null then 0
        when "Total_Attendance" ~ '^[0-9]+\.?[0-9]*$' then cast("Total_Attendance" as numeric)
        else 0
    end as total_attendance,
    _submission_time,
    "Place_1_Photo_URL",
    "Place_2_Photo_URL",
    "Place_3_Photo_URL",
    "Place_4_Photo_URL",
    "Search_Driver_Name",
    _validation_status,
    "Place_1___Attendance",
    "Place_2___Attendance",
    "Place_3___Attendance",
    "Place_4___Attendance",
    "Village_GPS__altitude",
    "Village_GPS__latitude",
    "Village_GPS__longitude",
    "Village_GPS__precision",
    "Time_of_Health_Education",
    "Was_Health_Worker_Present__",
    "Time_of_Health_Education_End_",
    "Topic_of_the_Health_Education",
    "Was_Health_Education_Conducted",
    "Health_Education_Given_By__Name_",
    case 
        when "Number_of_Places_with_Health_Education" is null then 0
        when "Number_of_Places_with_Health_Education" ~ '^[0-9]+\.?[0-9]*$' then cast("Number_of_Places_with_Health_Education" as numeric)
        else 0
    end as number_of_places_with_health_education,
    "Reason_Not_Conducted_Was_Health_Worker_Present_",
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta

from {{ source('staging_tribal_health_education', 'health_education_session_log') }}
{{ config(materialized='table') }}

/*
  Location Normalization Strategy:
  ─────────────────────────────────
  1. Seed / ref tables used:
       - ref('state_master')           → canonical Indian states/UTs
       - ref('maharashtra_district_master')   → 36 Maharashtra districts
       - ref('maharashtra_taluka_master')     → taluka ↔ district ↔ state mapping

  2. Special raw-value rules applied BEFORE master lookups:
       - native_state IN ('USA','New York','US','America', etc.)  → state_category = 'International'
       - native_district ILIKE '%out of maharashtra%'
         or native_district contains a known state name          → district treated as a state hint,
                                                                   district_cleaned set to NULL
       - native_state is NULL/blank but native_district matches a
         known MH district                                       → state inferred as 'Maharashtra'
       - native_tehsiltown value that matches a known district name → tehsil set to NULL (was district)

  3. Matching is case-insensitive and trims whitespace.
     Fuzzy variant handling is done via explicit CASE maps below.
     Extend the alias maps as new bad-data variants are discovered.
*/

with source_data as (
    select
        id::text                                           as participant_id,
        name::text                                         as name,
        gender::text                                       as gender,
        batch::text                                        as batch,
        workshop_details::text                             as workshop,
        nirman_krutee_nirman::text                         as workshop_type,
        workshops_attended                                 as workshops_attended,
        workshop_month_and_year::text                      as workshop_month_year,
        stream::text                                       as educational_stream,
        state::text                                        as native_state,
        district::text                                     as native_district,
        tehsiltown::text                                   as native_tehsiltown,
        point_2_workshop_name::text                        as point_2_workshop_name,
        point_2_workshop_invited::text                     as point_2_workshop_invited,
        point_2_workshop_attended::text                    as point_2_workshop_attended,
        point_3_workshop_name::text                        as point_3_workshop_name,
        point_3_workshop_invited::text                     as point_3_workshop_invited,
        point_3_workshop_attended::text                    as point_3_workshop_attended,
        case
            when trim(level_of_education) = ''              then null
            when upper(trim(level_of_education)) = 'NA'     then null
            when upper(trim(level_of_education)) = 'N/A'    then null
            when level_of_education is null                 then null
            else level_of_education::text
        end                                                as level_of_education,
        current_status_of_work::text                       as current_status_of_work,
        case
            when trim(person_years_of_social_action_as_on_31st_march_latest::text) = ''   then null
            when upper(trim(person_years_of_social_action_as_on_31st_march_latest::text)) = 'NA'  then null
            when upper(trim(person_years_of_social_action_as_on_31st_march_latest::text)) = 'N/A' then null
            when person_years_of_social_action_as_on_31st_march_latest is null             then null
            else round(person_years_of_social_action_as_on_31st_march_latest::numeric, 1)
        end                                                as person_years_social_action
    from {{ source('nirman', 'demographics_raw') }}
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1 : basic cleaning + work/education categorisation (unchanged logic)
-- ─────────────────────────────────────────────────────────────────────────────
cleaned_data as (
    select
        participant_id,
        trim(name)                   as name,
        trim(upper(gender))          as gender,
        case
            when trim(batch) = ''                             then null
            when upper(trim(batch)) in ('NA', 'N/A')         then null
            else nullif(regexp_replace(trim(batch), '[^0-9\\.]', '', 'g'), '')::numeric
        end                          as batch,
        trim(workshop)               as workshop,
        trim(workshop_type)          as workshop_type,
        trim(workshop_month_year)    as workshop_month_year,
        workshops_attended,
        trim(educational_stream)     as educational_stream_raw,
        trim(level_of_education)     as level_of_education,
        trim(current_status_of_work) as current_status_of_work_raw,
        person_years_social_action,

        -- raw location fields preserved for audit
        trim(native_state)           as raw_state,
        trim(native_district)        as raw_district,
        trim(native_tehsiltown)      as raw_tehsil,

        point_2_workshop_name,
        point_2_workshop_invited,
        point_2_workshop_attended,
        point_3_workshop_name,
        point_3_workshop_invited,
        point_3_workshop_attended,

        -- ── Current Status of Work ──────────────────────────────────────────
        case
            when upper(trim(current_status_of_work)) = 'DT'
                then 'Different Trajectory'
            when upper(trim(current_status_of_work)) = 'EDB'
                then 'Educational Break'
            when upper(trim(current_status_of_work)) = 'OW'
                then 'Other Work'
            when upper(trim(current_status_of_work)) = 'NOT NOW'
                 or lower(trim(current_status_of_work)) ilike '%not now%'
                then 'Previously SIW'
            when upper(trim(current_status_of_work)) = 'SIW'
                 or lower(trim(current_status_of_work)) ilike 'social impact work'
                then 'Social Impact Work'
            when upper(trim(current_status_of_work)) = 'NOT SURE'
                then 'Not Sure'
            else trim(current_status_of_work)
        end as current_status_of_work,

        -- ── Educational Stream 1 ─────────────────────────────────────────────
        case
            when lower(trim(educational_stream)) ilike '%medical%'
                then 'Medical'
            when lower(trim(educational_stream)) ilike '%engineering%'
                then 'Engineering'
            when lower(trim(educational_stream)) ilike 'arts%'
                 or lower(trim(educational_stream)) ilike '%art%'
                 or lower(trim(educational_stream)) ilike '%commerce%'
                 or lower(trim(educational_stream)) ilike '%science%'
                 or lower(trim(educational_stream)) ilike '%pharmancy%'
                then 'Arts/Commerce/Science/Pharmacy'
            else 'Other'
        end as educational_stream_1,

        -- ── Education Status ─────────────────────────────────────────────────
        case
            when lower(trim(current_status_of_work)) ilike '%student%'
                then 'Student'
            when upper(trim(current_status_of_work)) = 'SIW'
                 or lower(trim(current_status_of_work)) ilike '%social impact work%'
                 or upper(trim(current_status_of_work)) = 'NOT NOW'
                 or lower(trim(current_status_of_work)) ilike '%not now%'
                 or lower(trim(current_status_of_work)) ilike '%prev siw%'
                 or upper(trim(current_status_of_work)) = 'OW'
                 or lower(trim(current_status_of_work)) ilike '%other work%'
                 or upper(trim(current_status_of_work)) = 'EDB'
                 or lower(trim(current_status_of_work)) ilike '%educational break%'
                 or upper(trim(current_status_of_work)) = 'DT'
                 or lower(trim(current_status_of_work)) ilike '%different trajectory%'
                then 'Graduate'
            when (current_status_of_work is null
                  or trim(current_status_of_work) = ''
                  or lower(trim(current_status_of_work)) ilike '%not sure%')
                 and (
                     upper(trim(level_of_education)) = 'M'
                     or upper(trim(level_of_education)) = 'P'
                     or lower(trim(level_of_education)) ilike '%master%'
                     or lower(trim(level_of_education)) ilike '%phd%'
                 )
                then 'Graduate'
            else 'Not Known'
        end as education_status

    from source_data
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2 : normalise raw location values
--          Rules (applied in priority order):
--
--  A. STATE normalisation
--     A1. International markers (USA, New York, UK, …) → 'International'
--     A2. Match against state_master (case-insensitive)
--     A3. Known spelling aliases / abbreviations
--     A4. If raw_state is blank/null but raw_district matches a MH district → 'Maharashtra'
--     A5. If raw_state is blank/null but raw_district matches any Indian state → that state
--     A6. Otherwise → NULL
--
--  B. DISTRICT normalisation  (only meaningful for Maharashtra)
--     B1. 'Out of Maharashtra', 'Outside Maharashtra', state-name-in-district-field → NULL
--     B2. Match against maharashtra_district_master (case-insensitive)
--     B3. Known spelling aliases
--     B4. Otherwise → NULL
--
--  C. TEHSIL normalisation  (only meaningful when district is known)
--     C1. Value that matches a district name → NULL  (data-entry error)
--     C2. Match against maharashtra_taluka_master for the resolved district
--     C3. Otherwise → NULL
-- ─────────────────────────────────────────────────────────────────────────────

location_normalized as (
    select
        *,

        -- ════════════════════════════════════════════════════════════════
        -- A. RESOLVE STATE
        -- ════════════════════════════════════════════════════════════════
        case

            -- A1. Explicitly international
            when lower(trim(raw_state)) in (
                'usa', 'us', 'united states', 'united states of america',
                'new york', 'california', 'texas', 'new jersey',
                'uk', 'united kingdom', 'england', 'australia', 'canada',
                'germany', 'france', 'uae', 'dubai', 'singapore'
            )   then 'International'

            -- A2+A3. Indian states – exact + known aliases
            when lower(trim(raw_state)) in ('maharashtra', 'mh', 'maha')
                then 'Maharashtra'
            when lower(trim(raw_state)) in ('delhi', 'new delhi', 'ncr')
                then 'Delhi'
            when lower(trim(raw_state)) in ('up', 'uttar pradesh', 'u.p.')
                then 'Uttar Pradesh'
            when lower(trim(raw_state)) in ('mp', 'madhya pradesh', 'm.p.')
                then 'Madhya Pradesh'
            when lower(trim(raw_state)) in ('rajasthan', 'rj')
                then 'Rajasthan'
            when lower(trim(raw_state)) in ('gujarat', 'gj')
                then 'Gujarat'
            when lower(trim(raw_state)) in ('karnataka', 'karnatak', 'ktk')
                then 'Karnataka'
            when lower(trim(raw_state)) in ('andhra pradesh', 'ap', 'andhra')
                then 'Andhra Pradesh'
            when lower(trim(raw_state)) in ('telangana', 'ts', 'telangna')
                then 'Telangana'
            when lower(trim(raw_state)) in ('tamil nadu', 'tamilnadu', 'tn')
                then 'Tamil Nadu'
            when lower(trim(raw_state)) in ('kerala', 'kl')
                then 'Kerala'
            when lower(trim(raw_state)) in ('west bengal', 'wb', 'bengal')
                then 'West Bengal'
            when lower(trim(raw_state)) in ('bihar', 'br')
                then 'Bihar'
            when lower(trim(raw_state)) in ('jharkhand', 'jh')
                then 'Jharkhand'
            when lower(trim(raw_state)) in ('odisha', 'orissa', 'od')
                then 'Odisha'
            when lower(trim(raw_state)) in ('assam', 'as')
                then 'Assam'
            when lower(trim(raw_state)) in ('punjab', 'pb')
                then 'Punjab'
            when lower(trim(raw_state)) in ('haryana', 'hr')
                then 'Haryana'
            when lower(trim(raw_state)) in ('himachal pradesh', 'hp', 'himachal')
                then 'Himachal Pradesh'
            when lower(trim(raw_state)) in ('uttarakhand', 'uk', 'uttaranchal')
                then 'Uttarakhand'
            when lower(trim(raw_state)) in ('chhattisgarh', 'chattisgarh', 'cg')
                then 'Chhattisgarh'
            when lower(trim(raw_state)) in ('goa')
                then 'Goa'
            when lower(trim(raw_state)) in ('j&k', 'jammu & kashmir', 'jammu and kashmir', 'jk')
                then 'Jammu and Kashmir'
            when lower(trim(raw_state)) in ('chandigarh', 'ch')
                then 'Chandigarh'
            when lower(trim(raw_state)) in ('puducherry', 'pondicherry', 'py')
                then 'Puducherry'

            -- A4. State blank but district clearly is a Maharashtra district
            when (raw_state is null or trim(raw_state) = '')
                 and upper(trim(raw_district)) in (
                     'AHMEDNAGAR','AKOLA','AMRAVATI','AURANGABAD','BEED','BHANDARA',
                     'BULDHANA','CHANDRAPUR','DHULE','GADCHIROLI','GONDIA','HINGOLI',
                     'JALGAON','JALNA','KOLHAPUR','LATUR','MUMBAI CITY','MUMBAI SUBURBAN',
                     'MUMBAI','NAGPUR','NANDED','NANDURBAR','NASHIK','OSMANABAD',
                     'PALGHAR','PARBHANI','PUNE','RAIGAD','RATNAGIRI','SANGLI',
                     'SATARA','SINDHUDURG','SOLAPUR','THANE','WARDHA','WASHIM','YAVATMAL'
                 )
                then 'Maharashtra'

            -- A5. Raw_state is blank but raw_district contains a known Indian state name
            --     (handles cases like district = "Out of Maharashtra - Karnataka")
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%karnataka%'  then 'Karnataka'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%gujarat%'    then 'Gujarat'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%telangana%'  then 'Telangana'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%andhra%'     then 'Andhra Pradesh'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%rajasthan%'  then 'Rajasthan'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%delhi%'      then 'Delhi'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%madhya pradesh%' then 'Madhya Pradesh'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%uttar pradesh%'  then 'Uttar Pradesh'
            when (raw_state is null or trim(raw_state) = '')
                 and lower(trim(raw_district)) ilike '%goa%'            then 'Goa'

            -- A6. raw_state has a value that itself looks like a district name
            --     (someone typed "Nashik" in the state field)
            when upper(trim(raw_state)) in (
                'AHMEDNAGAR','AKOLA','AMRAVATI','AURANGABAD','BEED','BHANDARA',
                'BULDHANA','CHANDRAPUR','DHULE','GADCHIROLI','GONDIA','HINGOLI',
                'JALGAON','JALNA','KOLHAPUR','LATUR','MUMBAI CITY','MUMBAI SUBURBAN',
                'MUMBAI','NAGPUR','NANDED','NANDURBAR','NASHIK','OSMANABAD',
                'PALGHAR','PARBHANI','PUNE','RAIGAD','RATNAGIRI','SANGLI',
                'SATARA','SINDHUDURG','SOLAPUR','THANE','WARDHA','WASHIM','YAVATMAL'
            )   then 'Maharashtra'

            -- blank / null
            when raw_state is null or trim(raw_state) = '' then null

            else initcap(trim(raw_state))   -- keep as-is (best effort)
        end as state_normalized,

        -- ════════════════════════════════════════════════════════════════
        -- B. RESOLVE DISTRICT  (only for Maharashtra)
        -- ════════════════════════════════════════════════════════════════
        case

            -- B1. Explicit "out of Maharashtra" markers → NULL
            when lower(trim(raw_district)) ilike '%out of maharashtra%'     then null
            when lower(trim(raw_district)) ilike '%outside maharashtra%'    then null
            when lower(trim(raw_district)) ilike '%out of mh%'              then null

            -- B1b. Raw district is actually a state name (not a district)
            when upper(trim(raw_district)) in (
                'MAHARASHTRA','KARNATAKA','GUJARAT','TELANGANA',
                'ANDHRA PRADESH','RAJASTHAN','DELHI','MADHYA PRADESH',
                'UTTAR PRADESH','GOA','KERALA','TAMIL NADU','BIHAR',
                'JHARKHAND','ODISHA','ASSAM','PUNJAB','HARYANA',
                'UTTARAKHAND','CHHATTISGARH','WEST BENGAL','HIMACHAL PRADESH'
            )   then null

            -- B2+B3. Match MH districts (exact + common aliases)
            when upper(trim(raw_district)) in ('AHMEDNAGAR','AHMED NAGAR','AHMEDANAGAR')
                then 'Ahmednagar'
            when upper(trim(raw_district)) in ('AKOLA')
                then 'Akola'
            when upper(trim(raw_district)) in ('AMRAVATI','AMARAVATI')
                then 'Amravati'
            when upper(trim(raw_district)) in ('AURANGABAD','AURANGABAD (CHHATRAPATI SAMBHAJINAGAR)','CHHATRAPATI SAMBHAJINAGAR')
                then 'Aurangabad'
            when upper(trim(raw_district)) in ('BEED','BID')
                then 'Beed'
            when upper(trim(raw_district)) in ('BHANDARA')
                then 'Bhandara'
            when upper(trim(raw_district)) in ('BULDHANA','BULDANA')
                then 'Buldhana'
            when upper(trim(raw_district)) in ('CHANDRAPUR','CHANDA')
                then 'Chandrapur'
            when upper(trim(raw_district)) in ('DHULE','DHULIA')
                then 'Dhule'
            when upper(trim(raw_district)) in ('GADCHIROLI')
                then 'Gadchiroli'
            when upper(trim(raw_district)) in ('GONDIA','GONDIYA')
                then 'Gondia'
            when upper(trim(raw_district)) in ('HINGOLI')
                then 'Hingoli'
            when upper(trim(raw_district)) in ('JALGAON')
                then 'Jalgaon'
            when upper(trim(raw_district)) in ('JALNA')
                then 'Jalna'
            when upper(trim(raw_district)) in ('KOLHAPUR')
                then 'Kolhapur'
            when upper(trim(raw_district)) in ('LATUR')
                then 'Latur'
            when upper(trim(raw_district)) in ('MUMBAI CITY','MUMBAI (CITY)')
                then 'Mumbai City'
            when upper(trim(raw_district)) in ('MUMBAI SUBURBAN','MUMBAI (SUBURBAN)','MUMBAI SUB')
                then 'Mumbai Suburban'
            when upper(trim(raw_district)) in ('MUMBAI','BOMBAY')
                then 'Mumbai City'          -- default Mumbai → City; adjust if needed
            when upper(trim(raw_district)) in ('NAGPUR')
                then 'Nagpur'
            when upper(trim(raw_district)) in ('NANDED','NANDED-WAGHALA')
                then 'Nanded'
            when upper(trim(raw_district)) in ('NANDURBAR')
                then 'Nandurbar'
            when upper(trim(raw_district)) in ('NASHIK','NASIK')
                then 'Nashik'
            when upper(trim(raw_district)) in ('OSMANABAD','DHARASHIV')
                then 'Osmanabad'
            when upper(trim(raw_district)) in ('PALGHAR')
                then 'Palghar'
            when upper(trim(raw_district)) in ('PARBHANI')
                then 'Parbhani'
            when upper(trim(raw_district)) in ('PUNE','POONA')
                then 'Pune'
            when upper(trim(raw_district)) in ('RAIGAD','RAIGARH','KULABA')
                then 'Raigad'
            when upper(trim(raw_district)) in ('RATNAGIRI')
                then 'Ratnagiri'
            when upper(trim(raw_district)) in ('SANGLI')
                then 'Sangli'
            when upper(trim(raw_district)) in ('SATARA')
                then 'Satara'
            when upper(trim(raw_district)) in ('SINDHUDURG','SINDU DURG')
                then 'Sindhudurg'
            when upper(trim(raw_district)) in ('SOLAPUR','SHOLAPUR')
                then 'Solapur'
            when upper(trim(raw_district)) in ('THANE','THAN','THANA')
                then 'Thane'
            when upper(trim(raw_district)) in ('WARDHA')
                then 'Wardha'
            when upper(trim(raw_district)) in ('WASHIM','VASHIM')
                then 'Washim'
            when upper(trim(raw_district)) in ('YAVATMAL','YEOTMAL','YAWATMAL')
                then 'Yavatmal'

            when raw_district is null or trim(raw_district) = '' then null

            else null   -- unrecognised district → null (flagged via district_match_status)
        end as district_normalized,

        -- ════════════════════════════════════════════════════════════════
        -- C. RESOLVE TEHSIL
        -- ════════════════════════════════════════════════════════════════
        case
            -- C1. Tehsil value looks like a district name BUT only null it out
            --     when it does NOT match the participant's own resolved district.
            --     26 Maharashtra districts have a same-named headquarter taluka
            --     (Akola, Buldhana, Nashik, Nagpur, etc.) — those must be kept.
            --
            --     Districts that do NOT have a same-named taluka (safe to null):
            --       Ahmednagar, Kolhapur, Mumbai City, Mumbai Suburban,
            --       Nagpur (has Nagpur Urban / Nagpur Rural, not "Nagpur"),
            --       Pune (has Pune City, not "Pune"),
            --       Raigad, Sangli, Sindhudurg, Solapur
            --
            when upper(trim(raw_tehsil)) in (
                'AHMEDNAGAR','AHMED NAGAR',
                'KOLHAPUR',
                'MUMBAI CITY','MUMBAI SUBURBAN','MUMBAI','BOMBAY',
                'NAGPUR',          -- no plain "Nagpur" taluka; use Nagpur Urban/Rural
                'PUNE','POONA',    -- no plain "Pune" taluka; use Pune City / Haveli
                'RAIGAD',
                'SANGLI',
                'SINDHUDURG',
                'SOLAPUR'
            )   then null

            -- C1b. Tehsil contains a district name that is DIFFERENT from the resolved district
            --      e.g. tehsil = "Nashik" but district_normalized = "Pune" → that's wrong data
            when upper(trim(raw_tehsil)) in (
                'AKOLA','AMRAVATI','AMARAVATI','AURANGABAD','BEED','BID',
                'BHANDARA','BULDHANA','BULDANA','CHANDRAPUR','DHULE','DHULIA',
                'GADCHIROLI','GONDIA','GONDIYA','HINGOLI',
                'JALGAON','JALNA','LATUR',
                'NANDED','NANDURBAR','NASHIK','NASIK','OSMANABAD','DHARASHIV',
                'PALGHAR','PARBHANI','RATNAGIRI',
                'SATARA','THANE','WARDHA','WASHIM','VASHIM','YAVATMAL','YEOTMAL'
            )
            -- Only null it when it does NOT match the person's own resolved district
            and upper(trim(raw_tehsil)) not in (
                upper(coalesce(
                    case
                        when upper(trim(raw_district)) in ('AHMEDNAGAR','AHMED NAGAR','AHMEDANAGAR') then 'AHMEDNAGAR'
                        when upper(trim(raw_district)) in ('AKOLA')              then 'AKOLA'
                        when upper(trim(raw_district)) in ('AMRAVATI','AMARAVATI') then 'AMRAVATI'
                        when upper(trim(raw_district)) in ('AURANGABAD','AURANGABAD (CHHATRAPATI SAMBHAJINAGAR)','CHHATRAPATI SAMBHAJINAGAR') then 'AURANGABAD'
                        when upper(trim(raw_district)) in ('BEED','BID')         then 'BEED'
                        when upper(trim(raw_district)) in ('BHANDARA')           then 'BHANDARA'
                        when upper(trim(raw_district)) in ('BULDHANA','BULDANA') then 'BULDHANA'
                        when upper(trim(raw_district)) in ('CHANDRAPUR','CHANDA') then 'CHANDRAPUR'
                        when upper(trim(raw_district)) in ('DHULE','DHULIA')     then 'DHULE'
                        when upper(trim(raw_district)) in ('GADCHIROLI')         then 'GADCHIROLI'
                        when upper(trim(raw_district)) in ('GONDIA','GONDIYA')   then 'GONDIA'
                        when upper(trim(raw_district)) in ('HINGOLI')            then 'HINGOLI'
                        when upper(trim(raw_district)) in ('JALGAON')            then 'JALGAON'
                        when upper(trim(raw_district)) in ('JALNA')              then 'JALNA'
                        when upper(trim(raw_district)) in ('LATUR')              then 'LATUR'
                        when upper(trim(raw_district)) in ('NANDED','NANDED-WAGHALA') then 'NANDED'
                        when upper(trim(raw_district)) in ('NANDURBAR')          then 'NANDURBAR'
                        when upper(trim(raw_district)) in ('NASHIK','NASIK')     then 'NASHIK'
                        when upper(trim(raw_district)) in ('OSMANABAD','DHARASHIV') then 'OSMANABAD'
                        when upper(trim(raw_district)) in ('PALGHAR')            then 'PALGHAR'
                        when upper(trim(raw_district)) in ('PARBHANI')           then 'PARBHANI'
                        when upper(trim(raw_district)) in ('RATNAGIRI')          then 'RATNAGIRI'
                        when upper(trim(raw_district)) in ('SATARA')             then 'SATARA'
                        when upper(trim(raw_district)) in ('THANE','THAN','THANA') then 'THANE'
                        when upper(trim(raw_district)) in ('WARDHA')             then 'WARDHA'
                        when upper(trim(raw_district)) in ('WASHIM','VASHIM')    then 'WASHIM'
                        when upper(trim(raw_district)) in ('YAVATMAL','YEOTMAL','YAWATMAL') then 'YAVATMAL'
                        else upper(trim(raw_district))
                    end,
                'NOMATCH'))
            )   then null

            -- C2. Alias map: spelling variants + parenthetical suffixes → canonical master name
            --     Strategy:
            --       (a) strip common parenthetical locality hints e.g. "Purandhar (Saswad)" → "Purandar"
            --       (b) fix known mis-spellings
            --       (c) handle alternate/old names
            --     Exact-case-insensitive matches fall through to the taluka master join (C3).

            -- ── Pune district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('purandhar','purandhar (saswad)','purandar (saswad)','purandhar saswad')
                then 'Purandar'
            when lower(trim(raw_tehsil)) in ('pune city','pune urban','pune municipal')
                then 'Pune City'
            when lower(trim(raw_tehsil)) in ('haveli','havali','haveli (pune)')
                then 'Haveli'
            when lower(trim(raw_tehsil)) in ('khed (pune)','khed pune')
                then 'Khed'
            when lower(trim(raw_tehsil)) in ('shirur (pune)','shirur pune')
                then 'Shirur'

            -- ── Nagpur district ─────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('nagpur urban','nagpur city','nagpur (urban)','nagpur u')
                then 'Nagpur Urban'
            when lower(trim(raw_tehsil)) in ('nagpur rural','nagpur (rural)','nagpur r')
                then 'Nagpur Rural'

            -- ── Solapur district ────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('solapur north','north solapur','solapur (north)','solapur n')
                then 'Solapur North'
            when lower(trim(raw_tehsil)) in ('solapur south','south solapur','solapur (south)','solapur s')
                then 'Solapur South'

            -- ── Kolhapur district ───────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('karvir','kolhapur city','kolhapur (city)','karvir (kolhapur)')
                then 'Karvir'

            -- ── Mumbai City / Suburban ──────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('mumbai city','mumbai (city)','mumbai c')
                then 'Fort'             -- default city area to Fort taluka
            when lower(trim(raw_tehsil)) in ('mumbai suburban','mumbai (suburban)','mumbai sub','mumbai s')
                then 'Andheri'          -- default suburban to Andheri taluka

            -- ── Ahmednagar district ─────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('nagar','ahmednagar city','ahmednagar (nagar)')
                then 'Nagar'
            when lower(trim(raw_tehsil)) in ('shrirampur','shrirampur (ahmednagar)','srirampur')
                then 'Shrirampur'
            when lower(trim(raw_tehsil)) in ('sangamner','sangamaner')
                then 'Sangamner'

            -- ── Nashik district ─────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('nashik','nasik','nashik city')
                then 'Nashik'
            when lower(trim(raw_tehsil)) in ('trimbakeshwar','trimbak','tryambakeshwar')
                then 'Trimbakeshwar'
            when lower(trim(raw_tehsil)) in ('igatpuri','igatpuru')
                then 'Igatpuri'

            -- ── Amravati district ────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('achalpur','achalpur (amravati)','ellichpur')
                then 'Achalpur'
            when lower(trim(raw_tehsil)) in ('anjangaon surji','anjangaon','anjangaon-surji')
                then 'Anjangaon Surji'
            when lower(trim(raw_tehsil)) in ('chandurbazar','chandur bazar','chandur bazaar')
                then 'Chandurbazar'
            when lower(trim(raw_tehsil)) in ('nandgaon khandeshwar','nandgaon (khandeshwar)')
                then 'Nandgaon Khandeshwar'

            -- ── Aurangabad district ──────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('khuldabad','khuldabad (aurangabad)','khultabad')
                then 'Khuldabad'
            when lower(trim(raw_tehsil)) in ('phulambri','phulambari')
                then 'Phulambri'

            -- ── Buldhana district ────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('deulgaon raja','deulgaon-raja','deulgoan raja')
                then 'Deulgaon Raja'
            when lower(trim(raw_tehsil)) in ('jalgaon jamod','jalgaon (jamod)','jalgaon-jamod')
                then 'Jalgaon Jamod'
            when lower(trim(raw_tehsil)) in ('sindkhed raja','sindkhed-raja','shindkhed raja')
                then 'Sindkhed Raja'

            -- ── Chandrapur district ──────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('gondpimpri','gond pimpri')
                then 'Gondpimpri'
            when lower(trim(raw_tehsil)) in ('sindewahi','sindewahi (chandrapur)')
                then 'Sindewahi'

            -- ── Gadchiroli district ──────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('desaiganj','desai ganj','wadsa','desaiganj (wadsa)')
                then 'Desaiganj'
            when lower(trim(raw_tehsil)) in ('bhamragad','bhamragarh')
                then 'Bhamragad'

            -- ── Gondia district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('arjuni morgaon','arjuni-morgaon','arjunimorgaon')
                then 'Arjuni Morgaon'
            when lower(trim(raw_tehsil)) in ('sadak arjuni','sadak-arjuni')
                then 'Sadak Arjuni'

            -- ── Hingoli district ─────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('aundha nagnath','aundha-nagnath','aundha')
                then 'Aundha Nagnath'
            when lower(trim(raw_tehsil)) in ('kalamnuri','kalamnuri (hingoli)')
                then 'Kalamnuri'

            -- ── Jalgaon district ─────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('muktainagar','muktai nagar','faizpur')
                then 'Muktainagar'
            when lower(trim(raw_tehsil)) in ('bhusawal','bhusawad','bhusaval')
                then 'Bhusawal'

            -- ── Kolhapur district ────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('gaganbawada','gaganbavada','gagan bawada')
                then 'Gaganbawada'
            when lower(trim(raw_tehsil)) in ('hatkanangale','hatkananagale','hatkananangle')
                then 'Hatkanangale'
            when lower(trim(raw_tehsil)) in ('radhanagari','radhanagri')
                then 'Radhanagari'

            -- ── Latur district ───────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('shirur anantpal','shirur (anantpal)','shirur anantapal')
                then 'Shirur Anantpal'
            when lower(trim(raw_tehsil)) in ('ahmadpur','ahmedpur','ahmedapur')
                then 'Ahmadpur'

            -- ── Nanded district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('himayatnagar','himayat nagar')
                then 'Himayatnagar'

            -- ── Osmanabad district ───────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('osmanabad','dharashiv','dharasiv')
                then 'Osmanabad'

            -- ── Palghar district ─────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('vikramgad','vikramgarh')
                then 'Vikramgad'

            -- ── Raigad district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('sudhagad','sudhagad (pali)','sudhagad pali')
                then 'Sudhagad'
            when lower(trim(raw_tehsil)) in ('shrivardhan','shrivardhan (raigad)')
                then 'Shrivardhan'

            -- ── Ratnagiri district ───────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('khed (ratnagiri)','khed ratnagiri')
                then 'Khed'
            when lower(trim(raw_tehsil)) in ('sangameshwar','sangameshwar (ratnagiri)','sangameswar')
                then 'Sangameshwar'

            -- ── Sangli district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('kavathe mahankal','kavathe-mahankal','kavthe mahankal')
                then 'Kavathe Mahankal'
            when lower(trim(raw_tehsil)) in ('miraj','miraj (sangli)')
                then 'Miraj'

            -- ── Satara district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('mahabaleshwar','mahabaleswar','mahableshwar')
                then 'Mahabaleshwar'

            -- ── Sindhudurg district ──────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('sawantwadi','sawant wadi','savantvadi')
                then 'Sawantwadi'
            when lower(trim(raw_tehsil)) in ('vaibhavwadi','vaibhav wadi','vaibhavawadi')
                then 'Vaibhavwadi'
            when lower(trim(raw_tehsil)) in ('dodamarg','doda marg')
                then 'Dodamarg'

            -- ── Wardha district ──────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('hinganghat','hinganghaat')
                then 'Hinganghat'
            when lower(trim(raw_tehsil)) in ('samudrapur','samudrpur')
                then 'Samudrapur'

            -- ── Yavatmal district ────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('zari jamani','zari-jamani','zarijamani','jari jamani')
                then 'Zari Jamani'
            when lower(trim(raw_tehsil)) in ('kelapur','kellapur','pandharkawada')
                then 'Kelapur'
            when lower(trim(raw_tehsil)) in ('umarkhed','umerkhed','umred khed')
                then 'Umarkhed'

            -- ── Beed district ────────────────────────────────────────────────
            when lower(trim(raw_tehsil)) in ('shirur kasar','shirur (kasar)','shirur-kasar')
                then 'Shirur Kasar'
            when lower(trim(raw_tehsil)) in ('ambajogai','ambejogai')
                then 'Ambajogai'

            when raw_tehsil is null or trim(raw_tehsil) = '' then null

            -- C3. Pass through as-is — the taluka master join will validate it
            --     (handles all exact-match cases like Bhusawal, Amalner, Karad, etc.)
            else trim(raw_tehsil)
        end as tehsil_pre_normalized

    from cleaned_data
),

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3 : join tehsil against taluka master to confirm it belongs to the
--          resolved district; if it doesn't match, set to NULL
-- ─────────────────────────────────────────────────────────────────────────────
taluka_master as (
    select
        lower(trim(tehsil_name))  as tehsil_key,
        tehsil_name,
        district,
        state
    from {{ ref('maharashtra_taluka_master') }}
),

-- Two joins: (1) district-scoped (preferred), (2) unscoped fallback
taluka_scoped as (
    select
        ln.*,
        tm.tehsil_name as tehsil_matched_scoped
    from location_normalized ln
    left join taluka_master tm
        on lower(trim(ln.tehsil_pre_normalized)) = tm.tehsil_key
        and tm.district = ln.district_normalized
),

taluka_unscoped as (
    select
        participant_id,
        tehsil_matched_unscoped
    from (
        select
            ts.participant_id,
            tm.tehsil_name as tehsil_matched_unscoped,
            row_number() over (partition by ts.participant_id order by tm.district) as rn
        from taluka_scoped ts
        left join taluka_master tm
            on lower(trim(ts.tehsil_pre_normalized)) = tm.tehsil_key
        where ts.tehsil_matched_scoped is null
          and ts.tehsil_pre_normalized is not null
    ) ranked
    where rn = 1
),

location_final as (
    select
        ts.*,
        -- Use scoped match first; fall back to unscoped if district-scoped found nothing
        coalesce(ts.tehsil_matched_scoped, tu.tehsil_matched_unscoped) as tehsil_normalized,

        -- Audit / data-quality flags
        case
            when ts.state_normalized is null and (ts.raw_state is not null and trim(ts.raw_state) <> '')
                then 'Unmatched'
            when ts.state_normalized = 'International'
                then 'International'
            else 'OK'
        end as state_match_status,

        case
            when ts.district_normalized is null
                 and (ts.raw_district is not null and trim(ts.raw_district) <> '')
                 and lower(trim(ts.raw_district)) not ilike '%out of maharashtra%'
                 and ts.state_normalized = 'Maharashtra'
                then 'Unmatched'
            else 'OK'
        end as district_match_status

    from taluka_scoped ts
    left join taluka_unscoped tu using (participant_id)
)

-- ─────────────────────────────────────────────────────────────────────────────
-- FINAL SELECT
-- ─────────────────────────────────────────────────────────────────────────────
select
    participant_id,
    name,
    gender,
    batch,
    workshop,
    workshop_type,
    workshop_month_year,
    workshops_attended,
    educational_stream_raw          as educational_stream,
    educational_stream_1,
    current_status_of_work,
    person_years_social_action,
    education_status,

    -- Normalised location columns
    state_normalized                as native_state,
    district_normalized             as native_district,
    tehsil_normalized               as native_tehsiltown,

    -- Raw originals kept for audit / debugging
    raw_state                       as raw_native_state,
    raw_district                    as raw_native_district,
    raw_tehsil                      as raw_native_tehsil,

    -- Data quality flags
    state_match_status,
    district_match_status,

    point_2_workshop_name,
    point_2_workshop_invited,
    point_2_workshop_attended,
    point_3_workshop_name,
    point_3_workshop_invited,
    point_3_workshop_attended

from location_final
where participant_id is not null
order by participant_id
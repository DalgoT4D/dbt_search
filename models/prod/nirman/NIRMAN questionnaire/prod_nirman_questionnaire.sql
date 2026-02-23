{{ config(materialized='table') }}

-- This model creates a wide format with actual question text as column names
-- Uses the question lookup to create readable column headers

with base_data as (
    select
        participant_id,
        participant_name,
        workshop_name,
        batch,
        workshop_phase,
        
        -- Career questions (8)
    max(case when question_code = 'carrer_1' then coalesce(cast(response_int as text), response_text) end) as "Goverment Service",
    max(case when question_code = 'carrer_2' then coalesce(cast(response_int as text), response_text) end) as "Job in Private Industry",
    max(case when question_code = 'carrer_3' then coalesce(cast(response_int as text), response_text) end) as "Own Buisness",
    max(case when question_code = 'carrer_4' then coalesce(cast(response_int as text), response_text) end) as "Social Entrepreneurship/Own NGO",
    max(case when question_code = 'carrer_5' then coalesce(cast(response_int as text), response_text) end) as "Working in NGO",
    max(case when question_code = 'carrer_6' then coalesce(cast(response_int as text), response_text) end) as "Academic (College/University)",
    max(case when question_code = 'carrer_7' then coalesce(cast(response_int as text), response_text) end) as "Confused / Not Decided",
    max(case when question_code = 'carrer_8' then coalesce(cast(response_int as text), response_text) end) as "Other",
        
        -- Criteria questions (8)
    max(case when question_code = 'criteria_1' then coalesce(cast(response_int as text), response_text) end) as "Geographical Location",
    max(case when question_code = 'criteria_2' then coalesce(cast(response_int as text), response_text) end) as "Financial Security",
    max(case when question_code = 'criteria_3' then coalesce(cast(response_int as text), response_text) end) as "Work Satisfaction",
    max(case when question_code = 'criteria_4' then coalesce(cast(response_int as text), response_text) end) as "Cause of My work",
    max(case when question_code = 'criteria_5' then coalesce(cast(response_int as text), response_text) end) as "Carrer progress/growth",
    max(case when question_code = 'criteria_6' then coalesce(cast(response_int as text), response_text) end) as "Work Life Balance",
    max(case when question_code = 'criteria_7' then coalesce(cast(response_int as text), response_text) end) as "Financial Prosperity",
    max(case when question_code = 'criteria_8' then coalesce(cast(response_int as text), response_text) end) as "Job Security",
        
        -- Insecurities questions (10)
    max(case when question_code = 'insecurities_1' then coalesce(cast(response_int as text), response_text) end) as "Social Acceptability",
    max(case when question_code = 'insecurities_2' then coalesce(cast(response_int as text), response_text) end) as "Monetary Compensation",
    max(case when question_code = 'insecurities_3' then coalesce(cast(response_int as text), response_text) end) as "Response of Parents",
    max(case when question_code = 'insecurities_4' then coalesce(cast(response_int as text), response_text) end) as "Difficulty in finding the sutaible life partner",
    max(case when question_code = 'insecurities_5' then coalesce(cast(response_int as text), response_text) end) as "Underutilisation of my talent and skills",
    max(case when question_code = 'insecurities_6' then coalesce(cast(response_int as text), response_text) end) as "Less comfertable lifestyle as compared to my usual peers",
    max(case when question_code = 'insecurities_7' then coalesce(cast(response_int as text), response_text) end) as "Not being Able to create any visible impact",
    max(case when question_code = 'insecurities_8' then coalesce(cast(response_int as text), response_text) end) as "Feeling professionaly leftout",
    max(case when question_code = 'insecurities_9' then coalesce(cast(response_int as text), response_text) end) as "Lack of satisfaction at the end of life",
    max(case when question_code = 'insecurities_10' then coalesce(cast(response_int as text), response_text) end) as "Will my life be less Comfortable than it currently is",
        
        -- Social contribution
    max(case when question_code = 'social_contribution' then coalesce(cast(response_int as text), response_text) end) as "During my productive lifetime I plan to engage in social contribution",
        
    -- Questions (41)
    max(case when question_code = 'question_1' then coalesce(cast(response_int as text), response_text) end) as "I feel happy about myself. I like who I am.",
    max(case when question_code = 'question_2' then coalesce(cast(response_int as text), response_text) end) as "I have a good understanding about various social issues around me and their severity",
    max(case when question_code = 'question_3' then coalesce(cast(response_int as text), response_text) end) as "I believe that there is a purpose to my life",
    max(case when question_code = 'question_4' then coalesce(cast(response_int as text), response_text) end) as "I clearly know which sector I will be working in the long term",
    max(case when question_code = 'question_5' then coalesce(cast(response_int as text), response_text) end) as "I know what my values are",
    max(case when question_code = 'question_6' then coalesce(cast(response_int as text), response_text) end) as "I have mentor / facilitators I can talk to regarding myself, my confusions, my future plans",
    max(case when question_code = 'question_7' then coalesce(cast(response_int as text), response_text) end) as "I can take a specific stand about various social issues around me and in my vicinity",
    max(case when question_code = 'question_8' then coalesce(cast(response_int as text), response_text) end) as "I have the courage of going against the flow of conventional career options",
    max(case when question_code = 'question_9' then coalesce(cast(response_int as text), response_text) end) as "I am aware of various ways / approaches of solving social problems",
    max(case when question_code = 'question_10' then coalesce(cast(response_int as text), response_text) end) as "I do serious readinng to build my moral and political philosophy",
    max(case when question_code = 'question_11' then coalesce(cast(response_int as text), response_text) end) as "I usually act on my intentions to work towards social issues",
    max(case when question_code = 'question_12' then coalesce(cast(response_int as text), response_text) end) as "I know what is my 'Swa-dharma'",
    max(case when question_code = 'question_13' then coalesce(cast(response_int as text), response_text) end) as "I feel secure about my financial future",
    max(case when question_code = 'question_14' then coalesce(cast(response_int as text), response_text) end) as "I am a person who believes that it's my responsibility to take action for social change",
    max(case when question_code = 'question_15' then coalesce(cast(response_int as text), response_text) end) as "I have many likeminded friends who also believe in taking action for social change",
    max(case when question_code = 'question_16' then coalesce(cast(response_int as text), response_text) end) as "I have found a purpose for my life",
    max(case when question_code = 'question_17' then coalesce(cast(response_int as text), response_text) end) as "I know quite clearly what are my personal drives, motivations for social action",
    max(case when question_code = 'question_18' then coalesce(cast(response_int as text), response_text) end) as "I find connection between my personal career and social work & expect to contribute accordingly",
    max(case when question_code = 'question_19' then coalesce(cast(response_int as text), response_text) end) as "I am actively involved in the process of meaning-making for my life",
    max(case when question_code = 'question_20' then coalesce(cast(response_int as text), response_text) end) as "I have a larger social dream that I can relate to",
    max(case when question_code = 'question_21' then coalesce(cast(response_int as text), response_text) end) as "I feel that I belong to a diverse and caring community",
    max(case when question_code = 'question_22' then coalesce(cast(response_int as text), response_text) end) as "I understand the difference between social work activities and the social problem solving approach",
    max(case when question_code = 'question_23' then coalesce(cast(response_int as text), response_text) end) as "I know that even if I fail, there are people out there to support me",
    max(case when question_code = 'question_24' then coalesce(cast(response_int as text), response_text) end) as "I find social problem solving intellectually exciting",
    max(case when question_code = 'question_25' then coalesce(cast(response_int as text), response_text) end) as "I can imagine myself as an impactful social change-maker",
    max(case when question_code = 'question_26' then coalesce(cast(response_int as text), response_text) end) as "I believe engaging with society is crucial for me to live a meaningful life",
    max(case when question_code = 'question_27' then coalesce(cast(response_int as text), response_text) end) as "There are inspiring role models in the social field that I can look up to",
    max(case when question_code = 'question_28' then coalesce(cast(response_int as text), response_text) end) as "I feel confident about my potential to bring about positive social change",
    max(case when question_code = 'question_29' then coalesce(cast(response_int as text), response_text) end) as "I understand the difference between happiness and meaning",
    max(case when question_code = 'question_30' then coalesce(cast(response_int as text), response_text) end) as "I feel I am an emotionally mature adult",
    max(case when question_code = 'question_31' then coalesce(cast(response_int as text), response_text) end) as "I think I am quite privileged in being who I am today",
    max(case when question_code = 'question_32' then coalesce(cast(response_int as text), response_text) end) as "I clearly know the differences between my needs and my wants",
    max(case when question_code = 'question_33' then coalesce(cast(response_int as text), response_text) end) as "15 years from now I see myself as an example that will encourage other youth to engage in social action",
    max(case when question_code = 'question_34' then coalesce(cast(response_int as text), response_text) end) as "I have a guiding life philosophy or moral framework to steer me through life decisions",
    max(case when question_code = 'question_35' then coalesce(cast(response_int as text), response_text) end) as "I am confident in experimenting with my life",
    max(case when question_code = 'question_36' then coalesce(cast(response_int as text), response_text) end) as "I get plenty of opportunities to reflect on what is it that I really want to do",
    max(case when question_code = 'question_37' then coalesce(cast(response_int as text), response_text) end) as "All things considered, I am satisfied with my life as a whole these days",
    max(case when question_code = 'question_38' then coalesce(cast(response_int as text), response_text) end) as "I think there are definite rights and wrongs, morals are not relative",
    max(case when question_code = 'question_39' then coalesce(cast(response_int as text), response_text) end) as "I am comfortable in expressing my feelings",
    max(case when question_code = 'question_40' then coalesce(cast(response_int as text), response_text) end) as "I believe that engaging in a path of pro-social purpose will have a positive effect on my well-being",
    max(case when question_code = 'question_41' then coalesce(cast(response_int as text), response_text) end) as "I know what it means to be a flourishing youth",
        
    -- Finance questions (4)
    max(case when question_code = 'finance_1' then coalesce(cast(response_int as text), response_text) end) as "Monthly income at age 25",
    max(case when question_code = 'finance_2' then coalesce(cast(response_int as text), response_text) end) as "Monthly income at age 30",
    max(case when question_code = 'finance_3' then coalesce(cast(response_int as text), response_text) end) as "Monthly income as per Financial sheet",
    max(case when question_code = 'finance_4' then coalesce(cast(response_int as text), response_text) end) as "Monthly income Divided by 2"
        
    from {{ ref('int_nirman_questionnaire') }}
    group by participant_id, participant_name, workshop_name, batch, workshop_phase
)

select * from base_data
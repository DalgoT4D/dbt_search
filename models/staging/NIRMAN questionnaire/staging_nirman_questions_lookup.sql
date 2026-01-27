{{ config(materialized='table') }}

-- Create a lookup table mapping question codes to actual question text
-- This is based on the codes sheet provided

with question_mapping as (
    select 'carrer_1' as question_code, 'career' as category, 'Goverment Service' as question_text
    union all select 'carrer_2', 'career', 'Job in Private Industry'
    union all select 'carrer_3', 'career', 'Social Entrepreneurship/Own NGO'
    union all select 'carrer_4', 'career', 'Own Buisness'
    union all select 'carrer_5', 'career', 'Academic (College/University)'
    union all select 'carrer_6', 'career', 'Working in NGO'
    union all select 'carrer_7', 'career', 'Confused / Not Decided'
    union all select 'carrer_8', 'career', 'Other'
    
    union all select 'criteria_1', 'criteria', 'Geographical Location'
    union all select 'criteria_2', 'criteria', 'Financial Security'
    union all select 'criteria_3', 'criteria', 'Work Satisfaction'
    union all select 'criteria_4', 'criteria', 'Cause of My work'
    union all select 'criteria_5', 'criteria', 'Carrer progress/growth'
    union all select 'criteria_6', 'criteria', 'Work Life Balance'
    union all select 'criteria_7', 'criteria', 'Financial Prosperity'
    union all select 'criteria_8', 'criteria', 'Job Security'
    
    union all select 'insecurities_1', 'insecurities', 'Social Acceptability'
    union all select 'insecurities_2', 'insecurities', 'Monetary Compensation'
    union all select 'insecurities_3', 'insecurities', 'Response of Parents'
    union all select 'insecurities_4', 'insecurities', 'Difficulty in finding the sutaible life partner'
    union all select 'insecurities_5', 'insecurities', 'Underutilisation of my talent and skills'
    union all select 'insecurities_6', 'insecurities', 'Less comfertable lifestyle as compared to my usual peers'
    union all select 'insecurities_7', 'insecurities', 'Not being Able to create any visible impact'
    union all select 'insecurities_8', 'insecurities', 'Feeling professionaly leftout'
    union all select 'insecurities_9', 'insecurities', 'Lack of satisfaction at the end of life'
    union all select 'insecurities_10', 'insecurities', 'Will my life be less Comfortable than it currently is'
    
    union all select 'social_contribution', 'social_contribution', 'During my productive lifetime I plan to engage in social contribution'
    
    union all select 'question_1', 'questions', 'I feel happy about myself. I like who I am.'
    union all select 'question_2', 'questions', 'I have a good understanding about various social issues around me and their severity'
    union all select 'question_3', 'questions', 'I believe that there is a purpose to my life'
    union all select 'question_4', 'questions', 'I clearly know which sector I will be working in the long term'
    union all select 'question_5', 'questions', 'I know what my values are'
    union all select 'question_6', 'questions', 'I have mentor / facilitators I can talk to regarding myself, my confusions, my future plans'
    union all select 'question_7', 'questions', 'I can take a specific stand about various social issues around me and in my vicinity'
    union all select 'question_8', 'questions', 'I have the courage of going against the flow of conventional career options'
    union all select 'question_9', 'questions', 'I am aware of various ways / approaches of solving social problems'
    union all select 'question_10', 'questions', 'I do serious readinng to build my moral and political philosophy'
    union all select 'question_11', 'questions', 'I usually act on my intentions to work towards social issues'
    union all select 'question_12', 'questions', 'I know what is my ''Swa-dharma'''
    union all select 'question_13', 'questions', 'I feel secure about my financial future'
    union all select 'question_14', 'questions', 'I am a person who believes that its my responsibility to take action for social change'
    union all select 'question_15', 'questions', 'I have many likeminded friends who also believe in taking action for social change'
    union all select 'question_16', 'questions', 'I have found a purpose for my life'
    union all select 'question_17', 'questions', 'I know quite clearly what are my personal drives, motivations for social action'
    union all select 'question_18', 'questions', 'I find connection between my personal career and social work & expect to contribute accordingly'
    union all select 'question_19', 'questions', 'I am actively involved in the process of meaning-making for my life'
    union all select 'question_20', 'questions', 'I have a larger social dream that I can relate to'
    union all select 'question_21', 'questions', 'I feel that I belong to a diverse and caring community'
    union all select 'question_22', 'questions', 'I understand the difference between social work activities and the social problem solving approach'
    union all select 'question_23', 'questions', 'I know that even if I fail, there are people out there to support me'
    union all select 'question_24', 'questions', 'I find social problem solving intellectually exciting'
    union all select 'question_25', 'questions', 'I can imagine myself as an impactful social change-maker'
    union all select 'question_26', 'questions', 'I believe engaging with society is crucial for me to live a meaningful life'
    union all select 'question_27', 'questions', 'There are inspiring role models in the social field that I can look up to'
    union all select 'question_28', 'questions', 'I feel confident about my potential to bring about positive social change'
    union all select 'question_29', 'questions', 'I understand the difference between happiness and meaning'
    union all select 'question_30', 'questions', 'I feel I am an emotionally mature adult'
    union all select 'question_31', 'questions', 'I think I am quite privileged in being who I am today'
    union all select 'question_32', 'questions', 'I clearly know the differences between my needs and my wants'
    union all select 'question_33', 'questions', '15 years from now I see myself as an example that will encourage other youth to engage in social action'
    union all select 'question_34', 'questions', 'I have a guiding life philosophy or moral framework to steer me through life decisions'
    union all select 'question_35', 'questions', 'I am confident in experimenting with my life'
    union all select 'question_36', 'questions', 'I get plenty of opportunities to reflect on what is it that I really want to do'
    union all select 'question_37', 'questions', 'All things considered, I am satisfied with my life as a whole these days'
    union all select 'question_38', 'questions', 'I think there are definite rights and wrongs, morals are not relative'
    union all select 'question_39', 'questions', 'I am comfortable in expressing my feelings'
    union all select 'question_40', 'questions', 'I believe that engaging in a path of pro-social purpose will have a positive effect on my well-being'
    union all select 'question_41', 'questions', 'I know what it means to be a flourishing youth'
    
    union all select 'finance_1', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure At the age of 25 years'
    union all select 'finance_2', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure At the age of 30 years'
    union all select 'finance_3', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure as per Financial sheet'
    union all select 'finance_4', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure Divided by 2'
)

select
    question_code,
    category,
    question_text
from question_mapping
order by 
    case category
        when 'career' then 1
        when 'criteria' then 2
        when 'insecurities' then 3
        when 'social_contribution' then 4
        when 'questions' then 5
        when 'finance' then 6
    end,
    question_code
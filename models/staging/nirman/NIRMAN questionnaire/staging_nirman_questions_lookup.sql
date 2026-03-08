{{ config(materialized='table') }}

-- Create a lookup table mapping question codes to actual question text
-- This is based on the codes sheet provided; when new self-assessment or
-- other questionnaire columns are added to the source, append an additional
-- ``union all select ...`` row below with the new code and human-readable text.
-- The downstream wide model automatically pivots on whatever codes appear in
-- this table, so maintenance is limited to editing this file (or switching to
-- a seed/CSV if preferred).

with question_mapping as (
    select 'carrer_1' as question_code, 'career' as category, 'Goverment Service' as question_text, null as short_question_text
    union all select 'carrer_2', 'career', 'Job in Private Industry', null
    union all select 'carrer_3', 'career', 'Social Entrepreneurship/Own NGO', null
    union all select 'carrer_4', 'career', 'Own Buisness', null
    union all select 'carrer_5', 'career', 'Academic (College/University)', null
    union all select 'carrer_6', 'career', 'Working in NGO', null
    union all select 'carrer_7', 'career', 'Confused / Not Decided', null
    union all select 'carrer_8', 'career', 'Other', null
    
    union all select 'criteria_1', 'criteria', 'Geographical Location', null
    union all select 'criteria_2', 'criteria', 'Financial Security', null
    union all select 'criteria_3', 'criteria', 'Work Satisfaction', null
    union all select 'criteria_4', 'criteria', 'Cause of My work', null
    union all select 'criteria_5', 'criteria', 'Carrer progress/growth', null
    union all select 'criteria_6', 'criteria', 'Work Life Balance', null
    union all select 'criteria_7', 'criteria', 'Financial Prosperity', null
    union all select 'criteria_8', 'criteria', 'Fame / Recognition', null
    union all select 'criteria_9', 'criteria', 'Job Security', null
    
    union all select 'insecurities_1', 'insecurities', 'Social Acceptability', null
    union all select 'insecurities_2', 'insecurities', 'Monetary Compensation', null
    union all select 'insecurities_3', 'insecurities', 'Response of Parents', null
    union all select 'insecurities_4', 'insecurities', 'Difficulty in finding the sutaible life partner', null
    union all select 'insecurities_5', 'insecurities', 'Underutilisation of my talent and skills', null
    union all select 'insecurities_6', 'insecurities', 'Less comfertable lifestyle as compared to my usual peers', null
    union all select 'insecurities_7', 'insecurities', 'Not being Able to create any visible impact', null
    union all select 'insecurities_8', 'insecurities', 'Feeling professionaly leftout', null
    union all select 'insecurities_9', 'insecurities', 'Lack of satisfaction at the end of life', null
    union all select 'insecurities_10', 'insecurities', 'Will my life be less Comfortable than it currently is', null
    
    union all select 'social_contribution', 'social_contribution', 'During my productive lifetime I plan to engage in social contribution', null
    
    union all select 'question_1', 'questions', 'I feel happy about myself. I like who I am.', 'Self-Happiness'
    union all select 'question_2', 'questions', 'I have a good understanding about various social issues around me and their severity', 'Social Issues Awareness'
    union all select 'question_3', 'questions', 'I believe that there is a purpose to my life', 'Life Purpose Belief'
    union all select 'question_4', 'questions', 'I clearly know which sector I will be working in the long term', 'Long-term Sector'
    union all select 'question_5', 'questions', 'I know what my values are', 'Personal Values'
    union all select 'question_6', 'questions', 'I have mentor / facilitators I can talk to regarding myself, my confusions, my future plans', 'Mentorship Access'
    union all select 'question_7', 'questions', 'I can take a specific stand about various social issues around me and in my vicinity', 'Social Stand'
    union all select 'question_8', 'questions', 'I have the courage of going against the flow of conventional career options', 'Career Courage'
    union all select 'question_9', 'questions', 'I am aware of various ways / approaches of solving social problems', 'Social Problem Solving'
    union all select 'question_10', 'questions', 'I do serious readinng to build my moral and political philosophy', 'Moral Philosophy Reading'
    union all select 'question_11', 'questions', 'I usually act on my intentions to work towards social issues', 'Social Action'
    union all select 'question_12', 'questions', 'I know what is my ''Swa-dharma''', 'Swa-Dharma Knowledge'
    union all select 'question_13', 'questions', 'I feel secure about my financial future', 'Financial Security'
    union all select 'question_14', 'questions', 'I am a person who believes that it''s my responsibility to take action for social change', 'Social Change Responsibility'
    union all select 'question_15', 'questions', 'I have many likeminded friends who also believe in taking action for social change', 'Like-minded Friends'
    union all select 'question_16', 'questions', 'I have found a purpose for my life', 'Purpose Found'
    union all select 'question_17', 'questions', 'I know quite clearly what are my personal drives, motivations for social action', 'Social Motivation'
    union all select 'question_18', 'questions', 'I find connection between my personal career and social work & expect to contribute accordingly', 'Career-Social Connection'
    union all select 'question_19', 'questions', 'I am actively involved in the process of meaning-making for my life', 'Meaning-Making'
    union all select 'question_20', 'questions', 'I have a larger social dream that I can relate to', 'Social Dream'
    union all select 'question_21', 'questions', 'I feel that I belong to a diverse and caring community', 'Community Belonging'
    union all select 'question_22', 'questions', 'I understand the difference between social work activities and the social problem solving approach', 'Social Work vs Problem Solving'
    union all select 'question_23', 'questions', 'I know that even if I fail, there are people out there to support me', 'Support Network'
    union all select 'question_24', 'questions', 'I find social problem solving intellectually exciting', 'Intellectual Excitement'
    union all select 'question_25', 'questions', 'I can imagine myself as an impactful social change-maker', 'Change-Maker Vision'
    union all select 'question_26', 'questions', 'I believe engaging with society is crucial for me to live a meaningful life', 'Societal Engagement'
    union all select 'question_27', 'questions', 'There are inspiring role models in the social field that I can look up to', 'Role Models'
    union all select 'question_28', 'questions', 'I feel confident about my potential to bring about positive social change', 'Change Confidence'
    union all select 'question_29', 'questions', 'I understand the difference between happiness and meaning', 'Happiness vs Meaning'
    union all select 'question_30', 'questions', 'I feel I am an emotionally mature adult', 'Emotional Maturity'
    union all select 'question_31', 'questions', 'I think I am quite privileged in being who I am today', 'Privilege Awareness'
    union all select 'question_32', 'questions', 'I clearly know the differences between my needs and my wants', 'Needs vs Wants'
    union all select 'question_33', 'questions', '15 years from now I see myself as an example that will encourage other youth to engage in social action', 'Future Example'
    union all select 'question_34', 'questions', 'I have a guiding life philosophy or moral framework to steer me through life decisions', 'Life Philosophy'
    union all select 'question_35', 'questions', 'I am confident in experimenting with my life', 'Life Experimentation'
    union all select 'question_36', 'questions', 'I get plenty of opportunities to reflect on what is it that I really want to do', 'Reflection Opportunities'
    union all select 'question_37', 'questions', 'All things considered, I am satisfied with my life as a whole these days', 'Life Satisfaction'
    union all select 'question_38', 'questions', 'I think there are definite rights and wrongs, morals are not relative', 'Moral Absolutism'
    union all select 'question_39', 'questions', 'I am comfortable in expressing my feelings', 'Emotional Expression'
    union all select 'question_40', 'questions', 'I believe that engaging in a path of pro-social purpose will have a positive effect on my well-being', 'Pro-Social Well-being'
    union all select 'question_41', 'questions', 'I know what it means to be a flourishing youth', 'Flourishing Youth'
    union all select 'question_42', 'questions', 'I understand the scientific definition of purpose', 'Purpose Definition'
    union all select 'question_43', 'questions', 'Multiple life possibilities of my 20s fill me with excitement', 'Life Possibilities Excitement'
    union all select 'question_44', 'questions', 'Finding my purpose in life is the most urgent priority for me', 'Purpose Priority'
    union all select 'question_45', 'questions', 'I understand the difference between leading a ''happy life'' and a ''meaningful life''', 'Happy vs Meaningful Life'
    union all select 'question_46', 'questions', 'I understand the difference between goal and purpose', 'Goal vs Purpose'
    union all select 'question_47', 'questions', 'The more money one earns, the more happy they are', 'Money-Happiness Link'
    union all select 'question_48', 'questions', 'I understand how to apply ''categorical moral framework'' while making important life decisions', 'Moral Framework Application'
    union all select 'question_49', 'questions', 'The process of finding / working on my purpose fills me with excitement', 'Purpose Process Excitement'
    union all select 'question_50', 'questions', 'The question of ''how much money would I earn in life'' does not bother me', 'Money Indifference'
    union all select 'question_51', 'questions', 'From those to whom much is given, much is expected', 'Privilege Responsibility'
    union all select 'question_52', 'questions', 'I value the ''freedom to work on my chosen challenge'' more than other opportunities with even higher financial gains', 'Challenge Freedom'
    union all select 'question_53', 'questions', 'I understand the importance of purpose and the various advantages associated with it', 'Purpose Importance'
    union all select 'question_54', 'questions', 'Leading a life of ''social contribution'' is one of my deepest quests', 'Social Contribution Quest'
    union all select 'question_55', 'questions', 'I know how an ''impactful social organisation'' functions', 'Social Organization Knowledge'
    union all select 'question_56', 'questions', 'There are many ''social change makers'' that I know of and feel inspired by', 'Change Maker Inspiration'
    union all select 'question_57', 'questions', 'I can visualise possible ways in which I can contribute in the social sector', 'Social Contribution Vision'
    union all select 'question_58', 'questions', 'I know the difference between my ''minimum threshold income for financial security'' and my ''aspirational income''', 'Income Thresholds'
    union all select 'question_59', 'questions', 'I think it''s safe to drink alcohol if consumed within limits', 'Alcohol Safety'
    union all select 'question_60', 'questions', 'I have identified specific books to read in the coming year', 'Reading Plan'
    union all select 'question_61', 'questions', 'I plan to proactively engage in actions to reduce my ''carbon footprint''', 'Carbon Footprint Reduction'
    union all select 'question_62', 'questions', 'I have identified specific actions to work upon in the next 6 months regarding my own flourishing', 'Flourishing Actions'
    union all select 'question_63', 'questions', 'I know some specific actions to do in the next 6 months as part of my social contribution', 'Social Contribution Actions'
    union all select 'question_64', 'questions', 'I have identified few specific actions to pursue my purpose journey', 'Purpose Journey Actions'
    union all select 'question_81', 'questions', 'I know how to become a ''flourishing youth''', 'Flourishing Youth'
    union all select 'question_82', 'questions', 'I can visualize at least 5 different career paths for myself', 'Career Visualization'
    union all select 'question_83', 'questions', 'I productively spend my day in meaningful pursuits', 'Meaningful Pursuits'
    union all select 'question_84', 'questions', 'I am committed to bringing about social change', 'Social Change Commitment'
    union all select 'question_85', 'questions', 'I have a mentor(s) with whom I can discuss important life matters', 'Mentorship'
    union all select 'question_86', 'questions', 'I have a ''guiding philosophy'' to steer me through life', 'Guiding Philosophy'
    union all select 'question_88', 'questions', 'I can take a stand about various social issues around me', 'Social Stand'
    union all select 'question_89', 'questions', 'I know how to understand and analyse a social change intervention', 'Change Intervention Analysis'
    union all select 'question_90', 'questions', 'I feel anxious about fulfilling financial aspirations / ambitions of my life', 'Financial Anxiety'
    union all select 'question_91', 'questions', 'I understand the scientific definition of ''purpose''', 'Purpose Definition'
    union all select 'question_92', 'questions', 'I know the building blocks of effective social problem-solving', 'Problem-Solving Blocks'
    union all select 'question_93', 'questions', 'I am clear about the process of finding and pursuing my purpose in life', 'Purpose Process Clarity'
    union all select 'question_94', 'questions', 'I know clearly my personal drives and motivations for social action', 'Social Motivation'
    union all select 'question_95', 'questions', 'I can stay firm on my values even if they are against conventional norms', 'Value Firmness'
    union all select 'question_96', 'questions', 'I understand different approaches / methods of bringing about social change', 'Change Methods'
    union all select 'question_97', 'questions', 'I understand the importance and urgency of identifying my purpose in life', 'Purpose Urgency'
    union all select 'question_98', 'questions', 'I have a ''set of values'' that help me in making important life decisions', 'Value Set'
    union all select 'question_99', 'questions', 'I have a fair idea about what to do with my life', 'Life Direction'
    union all select 'question_100', 'questions', 'I have carefully identified a set of moral resolutions to follow in my daily living', 'Moral Resolutions'
    union all select 'question_102', 'questions', 'I can visualize various possible ways in which I can contribute to the social sector', 'Social Contribution Vision'
    union all select 'question_104', 'questions', 'All things considered, I am satisfied with my life as a whole', 'Overall Life Satisfaction'
    union all select 'question_105', 'questions', 'I have identified specific actions to do in the next 1 year to pursue my purpose journey', 'Purpose Actions 1 Year'
    union all select 'question_106', 'questions', 'I clearly know which sector I will be working in for most of my professional life', 'Professional Sector'
    union all select 'question_107', 'questions', 'I have planned specific actions to do in the next 1 year as part of my social change-making journey', 'Change Actions 1 Year'
    union all select 'question_108', 'questions', 'I am empowered to enact my responsibilities as an ''Active Citizen'' while dealing with the state', 'Active Citizenship'
    union all select 'question_109', 'questions', 'I have a clear sense of purpose in my life', 'Purpose Clarity'
    
    union all select 'finance_1', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure At the age of 25 years', null
    union all select 'finance_2', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure At the age of 30 years', null
    union all select 'finance_3', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure as per Financial sheet', null
    union all select 'finance_4', 'finance', 'Quote a figure of the monthly income (INR) that you think will make you feel financially secure Divided by 2', null
)

select
    question_code,
    category,
    question_text,
    short_question_text
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
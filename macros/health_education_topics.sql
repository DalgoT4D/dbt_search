{% macro get_health_education_topics() %}
    {#
        Single source of truth for all health education topics.

        To add a new topic:
          1. Add a new entry to this list — that's it.
          2. prod_tribal_healtheducation.sql requires NO changes.

        Each entry:
          - column_name : the boolean column produced in the prod table
          - keywords    : list of ILIKE patterns (without %)
                          All variants / spellings go here.
    #}
    {% set topics = [
        {
            "column_name": "topic_machchhardani",
            "label": "मच्‍छरदाणीचा वापर",
            "keywords": [
                "मच्‍छरदाणीचा वापर",
                "मच्छरदाणीचा वापर",
                "मच्‍छरदाणी",
                "मच्छरदाणी"
            ]
        },
        {
            "column_name": "topic_ushmaaghat",
            "label": "उष्माघात",
            "keywords": ["उष्माघात"]
        },
        {
            "column_name": "topic_kharuj",
            "label": "खरूज",
            "keywords": ["खरुज", "खरूज"]
        },
        {
            "column_name": "topic_gajkarn",
            "label": "गजकर्ण",
            "keywords": ["गजकर्ण"]
        },
        {
            "column_name": "topic_jaipur_foot_camp",
            "label": "जयपुर फुट कॅम्प",
            "keywords": ["जयपुर फुट"]
        },
        {
            "column_name": "topic_diarrhea",
            "label": "डायरिया",
            "keywords": ["डायरिया"]
        },
        {
            "column_name": "topic_back_pain",
            "label": "पाठ कंबरदुखी",
            "keywords": ["पाठ कंबरदुखी", "पाठ-कंबरदुखी", "पाठ- कंबरदुखी"]
        },
        {
            "column_name": "topic_water_purification",
            "label": "पाण्याचे शुद्धीकरण",
            "keywords": [
                "पाण्याचे शुद्धीकरण",
                "पाण्‍याचे शुद्धीकरण",
                "पाण्‍याचे शुध्‍दीकरण",
                "पाण्याचे शुध्दीकरण"
            ]
        },
        {
            "column_name": "topic_bp",
            "label": "बीपी",
            "keywords": ["बीपी"]
        },
        {
            "column_name": "topic_paralysis",
            "label": "लकवा",
            "keywords": ["लकवा"]
        }
    ] %}
    {{ return(topics) }}
{% endmacro %}


{% macro topic_flags(source_column) %}
    {#
        Renders one boolean column per topic defined in get_health_education_topics().
        Usage in a SELECT:  {{ topic_flags('health_education_topics') }}
    #}
    {% for topic in get_health_education_topics() %}
        (
            {% for kw in topic.keywords %}
                {{ source_column }} ilike '%{{ kw }}%'
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) as {{ topic.column_name }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
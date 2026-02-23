{% macro unpivot_questionnaire_columns(ref_model, column_prefix, start_num, end_num) %}
    {# 
    This macro generates UNION ALL statements to unpivot questionnaire columns
    
    Args:
        ref_model: The dbt ref() of the source model
        column_prefix: The prefix of columns to unpivot (e.g., 'career', 'question')
        start_num: Starting number (e.g., 1)
        end_num: Ending number (e.g., 41)
    
    Example usage:
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'question', 1, 41) }}
    #}
    
    {% for i in range(start_num, end_num + 1) %}
        select 
            participant_id,
            participant_name,
            workshop_name,
            workshop_phase,
            batch,
            '{{ column_prefix }}_{{ i }}' as question_code,
            {{ column_prefix }}_{{ i }} as response
        from {{ ref_model }}
        where {{ column_prefix }}_{{ i }} is not null
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
{% endmacro %}
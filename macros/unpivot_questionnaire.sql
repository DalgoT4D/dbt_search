{% macro unpivot_questionnaire_columns(ref_model, column_prefix, start_num, end_num) %}
    {# 
    Unpivot numbered questionnaire columns into rows.
    Also splits each response into response_int (when purely numeric) and
    response_text, so downstream models don't need to repeat that logic.

    Args:
        ref_model:      The dbt ref() of the source model
        column_prefix:  The prefix of columns to unpivot (e.g., 'carrer', 'question')
        start_num:      Starting number (e.g., 1)
        end_num:        Ending number (e.g., 142)

    Example usage:
        {{ unpivot_questionnaire_columns(ref('staging_nirman_questionnaire'), 'question', 1, 142) }}
    #}

    {% for i in range(start_num, end_num + 1) %}
        select 
            participant_id,
            participant_name,
            workshop_name,
            workshop_phase,
            batch,
            '{{ column_prefix }}_{{ i }}' as question_code,
            case when trim(cast({{ column_prefix }}_{{ i }} as text)) ~ '^\d+$'
                 then cast(trim(cast({{ column_prefix }}_{{ i }} as text)) as int)
                 else null end as response_int,
            cast({{ column_prefix }}_{{ i }} as text) as response_text
        from {{ ref_model }}
        where {{ column_prefix }}_{{ i }} is not null
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
{% endmacro %}
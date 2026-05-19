{% macro normalised_village_tribal(column_name) %}
    {# clean the input once; we use the same expression in two places #}
    {% set cleaned_input = "regexp_replace(trim(trailing '. ' from trim(" ~ column_name ~ ")), '\\s+', ' ')" %}

    case
        when {{ column_name }} is null then null
        else
            coalesce(
                -- 1: exact mapping from our seed table
                (
                    select canonical_name
                    from {{ ref('village_mapping') }}
                    where regexp_replace(trim(trailing '. ' from trim(raw_name)), '\\s+', ' ')
                          = {{ cleaned_input }}
                    limit 1
                ),

                -- 2: if nothing matches, return the cleaned string as-is
                {{ cleaned_input }}
            )
    end
{% endmacro %}

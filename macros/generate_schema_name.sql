{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {# Handle specific cases based on folder names or tags #}
        {% if 'elementary' in node.fqn %}
            {{ target.schema }}_elementary

        {% elif 'intermediate' in node.fqn and node.fqn.index('intermediate') + 1 < node.fqn | length %}
            {% set prefix = node.fqn[node.fqn.index('intermediate')] %}
            {{ target.schema }}_intermediate_{{ prefix | trim }}

        {% elif 'prod' in node.fqn and node.fqn.index('prod') + 1 < node.fqn | length %}
            {% set prefix = node.fqn[node.fqn.index('prod')] %}
            {{ target.schema }}_{{ prefix | trim }}

        {# Fallback to default schema if no specific case matches #}
        {% else %}
            {{ default_schema }}
        {% endif %}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
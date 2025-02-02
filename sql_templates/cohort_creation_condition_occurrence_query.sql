{% extends "base.sql.j2" %}

{% block inclusion_criteria %}
{% if inclusion_criteria.demographics %}
    {{ demographics_filter(inclusion_criteria.demographics) }}
{% endif %}
{% if inclusion_criteria.temporal_events %}
    {{ temporal_event_filter(inclusion_criteria.temporal_events) }}
{% endif %}
{% endblock %}

{% block exclusion_criteria %}
{% if exclusion_criteria %}
    AND NOT EXISTS (
        SELECT 1
        FROM condition_occurrence c
        JOIN person p ON c.person_id = p.person_id
        WHERE c.person_id = p.person_id
            {{ demographics_filter(exclusion_criteria.demographics) }}
            {{ temporal_event_filter(exclusion_criteria.temporal_events) }}
    )
{% endif %}
{% endblock %}

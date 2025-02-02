{% extends "base.sql.j2" %}

{% block join_clauses %}
{% if inclusion_criteria.temporal_events %}
    {{ temporal_event_filter(inclusion_criteria.temporal_events) }}
{% endif %}
{% endblock %}

{% block inclusion_criteria %}
{% if inclusion_criteria.demographics %}
    {{ demographics_filter(inclusion_criteria.demographics) }}
{% endif %}
{% endblock %}

{% block exclusion_criteria %}
{% if exclusion_criteria %}
    AND NOT EXISTS (
        SELECT 1
        FROM condition_occurrence ex
        JOIN person ep ON ex.person_id = ep.person_id
        WHERE ex.person_id = c.person_id
            {{ demographics_filter(exclusion_criteria.demographics) }}
            {{ temporal_event_filter(exclusion_criteria.temporal_events) }}
    )
{% endif %}
{% endblock %}

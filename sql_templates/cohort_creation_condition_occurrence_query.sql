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
    {% if exclusion_criteria.demographics %}
        {{ demographics_filter(exclusion_criteria.demographics) }}
    {% endif %}
    {% if exclusion_criteria.temporal_events %}
        {{ temporal_event_filter(exclusion_criteria.temporal_events, alias="ex") }}
    {% endif %}
{% endif %}
{% endblock %}

SELECT c.person_id, 
       c.condition_start_date as cohort_start_date, 
       c.condition_end_date as cohort_end_date
FROM condition_occurrence c
JOIN person p ON c.person_id = p.person_id
WHERE c.condition_concept_id = {{ inclusion_criteria.condition_occurrence.condition_concept_id }}
{% if inclusion_criteria.demographics %}
    {% if inclusion_criteria.demographics.gender == 'female' %}
      AND p.gender_concept_id = 8532
    {% elif inclusion_criteria.demographics.gender == 'male' %}
      AND p.gender_concept_id = 8507
    {% endif %}
    {% if inclusion_criteria.demographics.min_birth_year %}
      AND p.year_of_birth >= {{ inclusion_criteria.demographics.min_birth_year }}
    {% endif %}
    {% if inclusion_criteria.demographics.max_birth_year %}
      AND p.year_of_birth <= {{ inclusion_criteria.demographics.max_birth_year }}
    {% endif %}
{% endif %}

    {% if inclusion_criteria.temporal_events %}
    AND c.person_id IN (
        {% for event in inclusion_criteria.temporal_events %}
            {% if loop.index == 1 %}
            SELECT person_id
            FROM (
                SELECT person_id,
                       ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY visit_start_date ASC) AS event_instance
                FROM visit_occurrence
                WHERE visit_concept_id = {{ event.event_concept_id }}
            ) seq
            WHERE seq.event_instance = {{ event.event_instance }}
            {% else %}
            {% if event.operator == 'NOT' %}
            AND NOT EXISTS (
                SELECT person_id
                FROM (
                    SELECT person_id,
                           ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY visit_start_date ASC) AS event_instance
                    FROM visit_occurrence
                    WHERE visit_concept_id = {{ event.event_concept_id }}
                ) seq
                WHERE seq.event_instance = {{ event.event_instance }}
            )
            {% else %}
            {{ event.operator }}
            SELECT person_id
            FROM (
                SELECT person_id,
                       ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY visit_start_date ASC) AS event_instance
                FROM visit_occurrence
                WHERE visit_concept_id = {{ event.event_concept_id }}
            ) seq
            WHERE seq.event_instance = {{ event.event_instance }}
            {% endif %}
            {% endif %}
        {% endfor %}
    )
{% endif %}

{% if exclusion_criteria %}
AND NOT EXISTS (
    SELECT 1
    FROM condition_occurrence ex
    JOIN person ep ON ex.person_id = ep.person_id
    WHERE   ex.person_id = c.person_id
            AND ex.condition_concept_id = {{ exclusion_criteria.condition_occurrence.condition_concept_id }}
            {% if exclusion_criteria.demographics %}
                {% if exclusion_criteria.demographics.min_birth_year %}
                AND ep.year_of_birth >= {{ exclusion_criteria.demographics.min_birth_year }}
                {% endif %}
                {% if exclusion_criteria.demographics.max_birth_year %}
                AND ep.year_of_birth <= {{ exclusion_criteria.demographics.max_birth_year }}
                {% endif %}
            {% endif %}
)
{% endif %}


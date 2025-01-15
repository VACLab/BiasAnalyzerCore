SELECT c.person_id, 
       c.condition_start_date as cohort_start_date, 
       c.condition_end_date as cohort_end_date
FROM condition_occurrence c
JOIN person p ON c.person_id = p.person_id
WHERE 1=1
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
    {% for event_group in inclusion_criteria.temporal_events %}
        {% if event_group.operator == 'AND' %}
            {% for event in event_group.events %}
                {% if event.event_type == 'condition_occurrence' %}
                    {% if loop.first %}
                        AND c.person_id IN (
                    {% endif %}
                        SELECT person_id
                        FROM condition_occurrence
                        WHERE person_id = c.person_id
                            AND condition_concept_id = {{ event.event_concept_id }}
                    {% if loop.last %}
                        )
                    {% endif %}
                {% elif event.event_type == 'visit_occurrence' %}
                    {% if loop.first %}
                        AND c.person_id IN (
                    {% endif %}
                        SELECT person_id
                        FROM visit_occurrence
                        WHERE person_id = c.person_id
                          AND visit_concept_id = {{ event.event_concept_id }}
                          AND event_instance = {{ event.event_instance }}
                    {% if loop.last %}
                        )
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% elif event_group.operator == 'OR' %}
            {% for event in event_group.events %}
                {% if event.event_type == 'condition_occurrence' %}
                    {% if loop.first %}
                        AND c.person_id IN (
                    {% endif %}
                        SELECT person_id
                        FROM condition_occurrence
                        WHERE person_id = c.person_id
                          AND condition_concept_id = {{ event.event_concept_id }}
                    {% if loop.last %}
                        )
                    {% endif %}
                {% elif event.event_type == 'visit_occurrence' %}
                    {% if loop.first %}
                        AND c.person_id IN (
                    {% endif %}
                        SELECT person_id
                        FROM visit_occurrence
                        WHERE person_id = c.person_id
                          AND visit_concept_id = {{ event.event_concept_id }}
                          AND event_instance = {{ event.event_instance }}
                    {% if loop.last %}
                        )
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% elif event_group.operator == 'NOT' %}
            {% for event in event_group.events %}
                {% if event.event_type == 'condition_occurrence' %}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM condition_occurrence ex
                        JOIN person ep ON ex.person_id = ep.person_id
                        WHERE ex.person_id = c.person_id
                            AND ex.condition_concept_id = {{ event.event_concept_id }}
                    )
                {% elif event.event_type == 'visit_occurrence' %}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM visit_occurrence ex
                        JOIN person ep ON ex.person_id = ep.person_id
                        WHERE ex.person_id = c.person_id
                            AND ex.visit_concept_id = {{ event.event_concept_id }}
                    )
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}

{% if exclusion_criteria %}
AND NOT EXISTS (
    SELECT 1
    FROM condition_occurrence ex
    JOIN person ep ON ex.person_id = ep.person_id
    WHERE ex.person_id = c.person_id
        {% if exclusion_criteria.demographics %}
            {% if exclusion_criteria.demographics.min_birth_year %}
            AND ep.year_of_birth >= {{ exclusion_criteria.demographics.min_birth_year }}
            {% endif %}
            {% if exclusion_criteria.demographics.max_birth_year %}
            AND ep.year_of_birth <= {{ exclusion_criteria.demographics.max_birth_year }}
            {% endif %}
        {% endif %}
    {% if exclusion_criteria.temporal_events %}
        {% for event_group in exclusion_criteria.temporal_events %}
            {% if event_group.operator == 'AND' %}
                {% for event in event_group.events %}
                    {% if event.event_type == 'condition_occurrence' %}
                        AND ex.condition_concept_id = {{ event.event_concept_id }}
                    {% elif event.event_type == 'visit_occurrence' %}
                        AND ex.visit_concept_id = {{ event.event_concept_id }}
                        AND ex.event_instance = {{ event.event_instance }}
                    {% endif %}
                {% endfor %}
            {% elif event_group.operator == 'OR' %}
                {% for event in event_group.events %}
                    {% if event.event_type == 'condition_occurrence' %}
                        AND ex.condition_concept_id = {{ event.event_concept_id }}
                    {% elif event.event_type == 'visit_occurrence' %}
                        AND ex.visit_concept_id = {{ event.event_concept_id }}
                        AND ex.event_instance = {{ event.event_instance }}
                    {% endif %}
                {% endfor %}
            {% elif event_group.operator == 'NOT' %}
                {% for event in event_group.events %}
                    {% if event.event_type == 'condition_occurrence' %}
                        AND NOT EXISTS (
                            SELECT 1
                            FROM condition_occurrence ex
                            WHERE ex.person_id = c.person_id
                                AND ex.condition_concept_id = {{ event.event_concept_id }}
                        )
                    {% elif event.event_type == 'visit_occurrence' %}
                        AND NOT EXISTS (
                            SELECT 1
                            FROM visit_occurrence ex
                            WHERE ex.person_id = c.person_id
                                AND ex.visit_concept_id = {{ event.event_concept_id }}
                        )
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
)
{% endif %}

WITH ranked_events AS (
    SELECT
        person_id,
        condition_concept_id,
        ROW_NUMBER() OVER (
            PARTITION BY person_id, condition_concept_id
            ORDER BY condition_start_date ASC
        ) AS event_instance
    FROM condition_occurrence
),
ranked_visits AS (
    SELECT
        person_id,
        visit_concept_id,
        ROW_NUMBER() OVER (
            PARTITION BY person_id, visit_concept_id
            ORDER BY visit_start_date ASC
        ) AS event_instance
    FROM visit_occurrence
)

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
                    AND c.person_id IN (
                        SELECT person_id
                        FROM ranked_events
                        WHERE condition_concept_id = {{ event.event_concept_id }}
                            AND event_instance = {{ event.event_instance }}
                    )
                {% elif event.event_type == 'visit_occurrence' %}
                    AND c.person_id IN (
                        SELECT person_id
                        FROM ranked_visits
                        WHERE visit_concept_id = {{ event.event_concept_id }}
                          AND event_instance = {{ event.event_instance }}
                    )
                {% endif %}
            {% endfor %}
        {% elif event_group.operator == 'OR' %}
            {% for event in event_group.events %}
                {% if event.event_type == 'condition_occurrence' %}
                    AND c.person_id IN (
                        SELECT person_id
                        FROM ranked_events
                        WHERE condition_concept_id = {{ event.event_concept_id }}
                          AND event_instance = {{ event.event_instance }}
                    )
                {% elif event.event_type == 'visit_occurrence' %}
                    AND c.person_id IN (
                        SELECT person_id
                        FROM ranked_visits
                        WHERE visit_concept_id = {{ event.event_concept_id }}
                          AND event_instance = {{ event.event_instance }}
                    )
                {% endif %}
            {% endfor %}
        {% elif event_group.operator == 'NOT' %}
            {% for event in event_group.events %}
                {% if event.event_type == 'condition_occurrence' %}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM ranked_events ex
                        WHERE ex.person_id = c.person_id
                            AND ex.condition_concept_id = {{ event.event_concept_id }}
                            AND ex.event_instance = {{ event.event_instance }}
                    )
                {% elif event.event_type == 'visit_occurrence' %}
                    AND NOT EXISTS (
                        SELECT 1
                        FROM ranked_visits ex
                        WHERE ex.person_id = c.person_id
                          AND ex.visit_concept_id = {{ event.event_concept_id }}
                          AND ex.event_instance = {{ event.event_instance }}
                    )
                {% endif %}
            {% endfor %}
        {% elif event_group.operator == 'BEFORE' %}
            {% if event_group.events[0].event_type == 'date' %}
                {% set timestamp = event_group.events[0].timestamp %}
                {% if event_group.events[1].event_type == 'condition_occurrence' %}
                    AND EXISTS (
                        SELECT 1
                        FROM condition_occurrence e1
                        WHERE e1.person_id = c.person_id
                          AND e1.condition_concept_id = {{ event_group.events[1].event_concept_id }}
                          AND e1.condition_start_date < '{{ timestamp }}'
                    )
                {% elif event_group.events[1].event_type == 'visit_occurrence' %}
                    AND EXISTS (
                        SELECT 1
                        FROM visit_occurrence e1
                        WHERE e1.person_id = c.person_id
                          AND e1.visit_concept_id = {{ event_group.events[1].event_concept_id }}
                          AND e1.visit_start_date < '{{ timestamp }}'
                    )
                {% endif %}
            {% elif event_group.events[1].event_type == 'date' %}
                {% set timestamp = event_group.events[1].timestamp %}
                {% if event_group.events[0].event_type == 'condition_occurrence' %}
                    AND EXISTS (
                        SELECT 1
                        FROM condition_occurrence e1
                        WHERE e1.person_id = c.person_id
                          AND e1.condition_concept_id = {{ event_group.events[0].event_concept_id }}
                          AND e1.condition_start_date < '{{ timestamp }}'
                    )
                {% elif event_group.events[0].event_type == 'visit_occurrence' %}
                    AND EXISTS (
                        SELECT 1
                        FROM visit_occurrence e1
                        WHERE e1.person_id = c.person_id
                          AND e1.visit_concept_id = {{ event_group.events[0].event_concept_id }}
                          AND e1.visit_start_date < '{{ timestamp }}'
                    )
                {% endif %}
            {% else %}
                {% if event_group.events[0].event_type == 'condition_occurrence' and event_group.events[1].event_type == 'visit_occurrence' %}
                    AND EXISTS (
                        SELECT 1
                        FROM condition_occurrence e1
                        JOIN visit_occurrence e2 ON e1.person_id = e2.person_id
                        WHERE e1.person_id = c.person_id
                          AND e1.condition_concept_id = {{ event_group.events[0].event_concept_id }}
                          AND e2.visit_concept_id = {{ event_group.events[1].event_concept_id }}
                          AND e1.condition_start_date < e2.visit_start_date
                    )
                {% endif %}
            {% endif %}
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
                        AND c.person_id NOT IN (
                            SELECT person_id
                            FROM ranked_events
                            WHERE condition_concept_id = {{ event.event_concept_id }}
                              AND event_instance = {{ event.event_instance }}
                        )
                    {% elif event.event_type == 'visit_occurrence' %}
                        AND c.person_id NOT IN (
                            SELECT person_id
                            FROM ranked_visits
                            WHERE visit_concept_id = {{ event.event_concept_id }}
                              AND event_instance = {{ event.event_instance }}
                        )
                    {% endif %}
                {% endfor %}
            {% elif event_group.operator == 'OR' %}
                AND NOT EXISTS (
                    SELECT 1
                    FROM (
                    {% for event in event_group.events %}
                        {% if event.event_type == 'condition_occurrence' %}
                            AND c.person_id NOT IN (
                            SELECT person_id
                            FROM ranked_events
                            WHERE condition_concept_id = {{ event.event_concept_id }}
                            AND event_instance = {{ event.event_instance }}
                            )
                        {% elif event.event_type == 'visit_occurrence' %}
                            AND c.person_id NOT IN (
                            SELECT person_id
                            FROM ranked_visits
                            WHERE visit_concept_id = {{ event.event_concept_id }}
                            AND event_instance = {{ event.event_instance }}
                            )
                        {% endif %}
                        {% if not loop.last %}
                            UNION ALL
                        {% endif %}
                    {% endfor %}
                    ) exclusions
                    WHERE exclusions.person_id = c.person_id
                )
            {% elif event_group.operator == 'NOT' %}
                {% for event in event_group.events %}
                    {% if event.event_type == 'condition_occurrence' %}
                        AND NOT EXISTS (
                            SELECT 1
                            FROM ranked_events ex
                            WHERE ex.person_id = c.person_id
                                AND ex.condition_concept_id = {{ event.event_concept_id }}
                                AND ex.event_instance = {{ event.event_instance }}
                        )
                    {% elif event.event_type == 'visit_occurrence' %}
                        AND NOT EXISTS (
                            SELECT 1
                            FROM ranked_visits ex
                            WHERE ex.person_id = c.person_id
                                AND ex.visit_concept_id = {{ event.event_concept_id }}
                                AND ex.event_instance = {{ event.event_instance }}
                        )
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endif %}
)
{% endif %}

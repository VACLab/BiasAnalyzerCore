SELECT c.person_id, 
       c.condition_start_date as cohort_start_date, 
       c.condition_end_date as cohort_end_date
FROM condition_occurrence c
JOIN person p ON c.person_id = p.person_id
WHERE c.condition_concept_id = {{ inclusion_criteria.condition_occurrence.condition_concept_id }}
{% if inclusion_criteria.condition_occurrence.gender == 'female' %}
  AND p.gender_concept_id = 8532
{% elif inclusion_criteria.condition_occurrence.gender == 'male' %}
  AND p.gender_concept_id = 8507
{% endif %}
{% if inclusion_criteria.condition_occurrence.min_birth_year %}
  AND p.year_of_birth >= {{ inclusion_criteria.condition_occurrence.min_birth_year }}
{% endif %}
{% if inclusion_criteria.condition_occurrence.max_birth_year %}
  AND p.year_of_birth <= {{ inclusion_criteria.condition_occurrence.max_birth_year }}
{% endif %}

{% if exclusion_criteria %}
AND NOT EXISTS (
    SELECT 1
    FROM condition_occurrence ex
    JOIN person ep ON ex.person_id = ep.person_id
    WHERE   ex.person_id = c.person_id
            AND ex.condition_concept_id = {{ exclusion_criteria.condition_occurrence.condition_concept_id }}
            {% if exclusion_criteria.condition_occurrence.min_birth_year %}
            AND ep.year_of_birth >= {{ exclusion_criteria.condition_occurrence.min_birth_year }}
            {% endif %}
            {% if exclusion_criteria.condition_occurrence.max_birth_year %}
            AND ep.year_of_birth <= {{ exclusion_criteria.condition_occurrence.max_birth_year }}
            {% endif %}
)
{% endif %}


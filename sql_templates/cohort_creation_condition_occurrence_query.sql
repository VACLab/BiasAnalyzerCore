SELECT c.person_id, 
       c.condition_start_date as cohort_start_date, 
       c.condition_end_date as cohort_end_date
FROM condition_occurrence c
JOIN person p ON c.person_id = p.person_id
WHERE c.condition_concept_id = {{ condition_occurrence.condition_concept_id }}
{% if condition_occurrence.gender == 'female' %}
  AND p.gender_concept_id = 8532
{% elif condition_occurrence.gender == 'male' %}
  AND p.gender_concept_id = 8507
{% endif %}
{% if condition_occurrence.min_birth_year %}
  AND p.year_of_birth >= {{ condition_occurrence.min_birth_year }}
{% endif %}
{% if condition_occurrence.max_birth_year %}
  AND p.year_of_birth <= {{ condition_occurrence.max_birth_year }}
{% endif %}

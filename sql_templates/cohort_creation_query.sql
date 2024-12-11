SELECT c.person_id, 
       c.condition_start_date as cohort_start_date, 
       c.condition_end_date as cohort_end_date
FROM condition_occurrence c 
JOIN person p ON c.person_id = p.person_id
WHERE c.condition_concept_id = {{ condition_concept_id }}
{% if gender_concept_id %} AND p.gender_concept_id = {{ gender_concept_id }} {% endif %}
{% if min_birth_year %} AND p.year_of_birth > {{ min_birth_year }} {% endif %}

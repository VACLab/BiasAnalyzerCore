from healthdatabias.api import BIAS


if __name__ == '__main__':
    try:
        bias = BIAS()
        bias.set_config('/home/hongyi/HealthDataBias/config.yaml')
        bias.set_root_omop()
        baseline_cohort_query = ('SELECT c.person_id, c.condition_start_date as cohort_start_date, '
                                 'c.condition_end_date as cohort_end_date '
                                 'FROM condition_occurrence c JOIN '
                                 'person p ON c.person_id = p.person_id '
                                 'WHERE c.condition_concept_id = 37311061 '
                                 'AND p.gender_concept_id = 8532 AND p.year_of_birth > 2002')
        bias.create_cohort('COVID-19 patients', 'COVID-19 patients',
                           baseline_cohort_query, 'system')
        cohort_def = bias.get_cohort_definitions()
        print(f'cohort_definition: {cohort_def}')
        cohort = bias.get_cohort(cohort_def[0]['id'], count=5)
        print(f'The first five records in the cohort {cohort_def[0]["id"]}: {cohort}')
        cohort_stats = bias.get_cohort_basic_stats(cohort_def[0]['id'])
        print(f'the cohort {cohort_def[0]["id"]} stats: {cohort_stats}')
        cohort_age_dists = bias.get_cohort_age_distributions(cohort_def[0]['id'])
        print(f'the cohort {cohort_def[0]["id"]} age distributions: {cohort_age_dists}')
        cohort_gender_dists = bias.get_cohort_gender_distributions(cohort_def[0]['id'])
        print(f'the cohort {cohort_def[0]["id"]} gender distributions: {cohort_gender_dists}')
    finally:
        bias.cleanup()
        print('done')

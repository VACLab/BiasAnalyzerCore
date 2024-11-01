from healthdatabias.api import BIAS
import pandas as pd


if __name__ == '__main__':
    bias = None
    try:
        bias = BIAS()
        bias.set_config('/home/hongyi/HealthDataBias/config.yaml')
        bias.set_root_omop()
        print(f'domains and vocabularies: {bias.get_domains_and_vocabularies()}')
        concepts = bias.get_concepts("COVID-19", "Condition", "SNOMED")
        print(f'concepts for COVID-19: {concepts}')
        parent_concept_tree, children_concept_tree = bias.get_concept_hierarchy(37311061)
        print(f'parent concept hierarchy for COVID-19 in text format:')
        print(bias.display_concept_tree(parent_concept_tree))
        print(f'children concept hierarchy for COVID-19 in text format:')
        print(bias.display_concept_tree(children_concept_tree))
        print(f'parent concept hierarchy for COVID-19 in widget tree format:')
        bias.display_concept_tree(parent_concept_tree,  show_in_text_format=False)
        print(f'children concept hierarchy for COVID-19 in widget tree format:')
        bias.display_concept_tree(children_concept_tree, show_in_text_format=False)
        baseline_cohort_query = ('SELECT c.person_id, c.condition_start_date as cohort_start_date, '
                                 'c.condition_end_date as cohort_end_date '
                                 'FROM condition_occurrence c JOIN '
                                 'person p ON c.person_id = p.person_id '
                                 'WHERE c.condition_concept_id = 37311061 '
                                 'AND p.gender_concept_id = 8532 AND p.year_of_birth > 2000')
        cohort_data = bias.create_cohort('COVID-19 patients', 'COVID-19 patients',
                                         baseline_cohort_query, 'system')
        if cohort_data:
            md = cohort_data.metadata
            print(f'cohort_definition: {md}')
            print(f'The first five records in the cohort {cohort_data.data[:5]}')
            print(f'the cohort stats: {cohort_data.get_stats()}')
            print(f'the cohort age stats: {cohort_data.get_stats("age")}')
            print(f'the cohort gender stats: {cohort_data.get_stats("gender")}')
            print(f'the cohort race stats: {cohort_data.get_stats("race")}')
            print(f'the cohort ethnicity stats: {cohort_data.get_stats("ethnicity")}')
            print(f'the cohort age distributions: {cohort_data.get_distributions("age")}')

            compare_stats = bias.compare_cohorts(cohort_data.metadata['id'], cohort_data.metadata['id'])
            print(f'compare_stats: {compare_stats}')

    finally:
        if bias is not None:
            bias.cleanup()
        print('done')

from biasanalyzer.api import BIAS
import time
import pandas as pd

def condition_cohort_test(bias_obj):
    baseline_cohort_query = ('SELECT c.person_id, c.condition_start_date as cohort_start_date, '
                             'c.condition_end_date as cohort_end_date '
                             'FROM condition_occurrence c JOIN '
                             'person p ON c.person_id = p.person_id '
                             'WHERE c.condition_concept_id = 37311061 '
                             'AND p.gender_concept_id = 8532 AND p.year_of_birth > 2000')
    cohort_data = bias_obj.create_cohort('COVID-19 patients', 'COVID-19 patients',
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
        t1 = time.time()
        cohort_concepts = cohort_data.get_concept_stats()

        print(f'the cohort concept stats: {pd.DataFrame(cohort_concepts["condition_occurrence"])}')
        print(f'the time taken to get cohort concept stats is {time.time() - t1}s')
        compare_stats = bias_obj.compare_cohorts(cohort_data.metadata['id'], cohort_data.metadata['id'])
        print(f'compare_stats: {compare_stats}')
    return cohort_data


def concept_test(bias_obj):
    print(f'domains and vocabularies: {bias_obj.get_domains_and_vocabularies()}')
    concepts = bias_obj.get_concepts("COVID-19")
    concepts = bias_obj.get_concepts("COVID-19", "Condition", "SNOMED")
    print(f'concepts for COVID-19 in Condition domain with SNOMED vocabulary: {concepts}')
    concepts = bias_obj.get_concepts("COVID-19", domain="Condition")
    print(f'concepts for COVID-19 in Condition domain: {concepts}')
    concepts = bias_obj.get_concepts("COVID-19", vocabulary="SNOMED")
    print(f'concepts for COVID-19 in SNOMED vocabulary: {concepts}')

    parent_concept_tree, children_concept_tree = bias_obj.get_concept_hierarchy(37311061)
    print('parent concept hierarchy for COVID-19 in text format:')
    print(bias_obj.display_concept_tree(parent_concept_tree))
    print('children concept hierarchy for COVID-19 in text format:')
    print(bias_obj.display_concept_tree(children_concept_tree))
    print(f'parent concept hierarchy for COVID-19 in widget tree format:')
    bias_obj.display_concept_tree(parent_concept_tree, show_in_text_format=False)
    print(f'children concept hierarchy for COVID-19 in widget tree format:')
    bias_obj.display_concept_tree(children_concept_tree, show_in_text_format=False)
    return


if __name__ == '__main__':
    bias = None
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    try:
        bias = BIAS()
        bias.set_config('/home/hongyi/BiasAnalyzer/config.yaml')
        bias.set_root_omop()

        condition_cohort_test(bias)

    finally:
        if bias is not None:
            bias.cleanup()
        print('done')

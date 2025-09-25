[![test](https://github.com/VACLab/BiasAnalyzer/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/VACLab/BiasAnalyzer/actions/workflows/test.yml)
<h1>
  <img src="./assets/logo.png" alt="Project Logo" width="50" style="vertical-align: middle; margin-right: 10px;">
  BiasAnalyzer (BA)
</h1>

BiasAnalyzer is a Python package that enables users to track, quantify, analyze, and communicate bias in cohort 
selection. In this context, bias is viewed as lack of representation within a cohort compared to a baseline population.

⚠️ **Status: Alpha - Work in Progress**  
This project is in an early alpha stage and under active development. It may undergo significant changes before its first 
official release.

To import the BiasAnalyzer package and use the BIAS API, you can follow the steps outlined below. Sample commands or 
code snippets are also provided in each step for easy reference.

- Install the BiasAnalyzer python package from [pypi](https://pypi.org/) or 
install it directly from this github repo. For example, run 
`pip install git+https://github.com/vaclab/BiasAnalyzer.git`
to install the python package from this github repo. 
- Run `from biasanalyzer.api import BIAS` to import the module API.
- Run `bias = BIAS()` to create an object of the imported BIAS class.
- Create a config.yaml file for specifying OMOP database connection configuration information. 
The config.yaml file must include root_omop_cdm_database key. 
- [A test OMOP database configuration yaml file](https://github.com/VACLab/BiasAnalyzer/blob/main/tests/assets/config/test_config.yaml) 
can serve as an example. Another config.yaml example for connecting to a OMOP postgreSQL database 
is also copied below for reference.
  ```angular2html
  root_omop_cdm_database:
    database_type: postgresql
    username: <username>
    password: <password>
    hostname: 172.25.15.19
    database: vaclab
    port: 5432
  ```
- Run `bias.set_config('/<config_path>/config.yaml')` to load config.yml file in the BIAS object. 
- Run `bias.set_root_omop()` to connect to the OMOP CDM database specified in config.yml file in read-only mode.

Now that you have connected to your OMOP CDM database using the BiasAnalyzer API, you can start to use the APIs 
to explore your data, create baseline and study patient cohorts, and compare cohorts for tracking, quantifying, 
analyzing, and communicating bias in cohort selection. The APIs currently available are summarized below for easy reference.
- Call `domains = bias.get_domains_and_vocabularies()` to get a list of domain and vocabulary dictionary items with domain_id and 
vocabulary_id keys included in each item in the OMOP CDM database. 
- Call `concepts = bias.get_concepts("search_term (e.g., COVID-19)", "domain_id (e.g., Condition)", "vocabulary_id (e.g., SNOMED)")` 
to get a list of concept dictionary items with concept_id, concept_name, valid_start_date, and valid_end_date keys 
included in each item. Note that you can provide only domain_id or only vocabulary_id input parameter to get all
concepts in the input domain or the input vocabulary, or provide both domain_id and vocabulary_id input parameters to 
get all concepts in the input domain and vocabulary. However, you cannot leave both parameters out and 
will have to specify at least one of the input parameters to limit the number of concepts being returned.  
- Call `parent_concept_tree, children_concept_tree = bias.get_concept_hierarchy(concept_id such as 37311061 for COVID-19)`
to get parent and children concept hierarchical tree for a specific input concept. The parent and children concept hierarchical 
tree are represented in a nesting dictionary with the input concept as the leaf and root in the hierarchy, respectively. 
In the nesting dictionary, the concept id serves as a key and a dictionary as a value where the dictionary value includes
the 'details' key containing detailed attributes about the concept_id, and the 'children' key or 'parents' key 
containing a list of the concept's children or parents in the hierarchy. 
- Call `bias.display_concept_tree(parent_concept_tree)` and `bias.display_concept_tree(children_concept_tree)` to display 
the concept hierarchical tree in an indented text format. If ipytree widget is installed and supported in a Jupyter notebook 
environment, you can set `show_in_text_format` input parameter to `False` 
(e.g., call `bias.display_concept_tree(parent_concept_tree,  show_in_text_format=False)`) to leverage the tree widget for displaying 
the hierarchy in a tree that can be expanded and collapsed on demand interactively.   

In addition to exploring the concepts using BiasAnalyzer APIs, the main functionalities of the BiasAnalyzer is 
to enable users to track, quantify, analyze, and communicate bias in cohort selection. The cohort-related APIs 
currently available are summarized below for your easy reference.

- You can create a baseline or study cohort using the `create_cohort()` API as shown below:
  ```angular2html
  baseline_cohort = bias.create_cohort("cohort_name", "cohort_description", 
  "SQL query or a yaml file name with full path", 'created_by_name')       
  ```
  The SQL query string must be a valid SQL query statement that can be executed in the OMOP CDM database. Here is 
a SQL query example for your reference: 
`SELECT person_id, condition_start_date as cohort_start_date, condition_end_date as cohort_end_date, FROM condition_occurrence WHERE condition_concept_id = 37311061`. 
As you can see, creating such a SQL query statement requires expertise about SQL query and OMOP CDM database tables 
which most users don't possess. An alternative method for cohort creation is to create a YAML file 
that lists inclusion and exclusion criteria for creating a specific cohort declaratively. You can refer to 
several YAML file examples for creating cohort in this [test folder](https://github.com/VACLab/BiasAnalyzer/tree/main/tests/assets/cohort_creation)
- After a cohort is created, a cohort object, e.g., baseline_cohort, is returned. You can then get metadata, 
data, statistics, and distributions of the cohort by accessing properties and methods of the created cohort objects. 
The following code snippets show some examples.
  ```angular2html
  baseline_cohort_def = baseline_cohort.metadata
  baseline_cohort_data = baseline_cohort.data
  cohort_stats = baseline_cohort.get_stats()
  cohort_age_stats = baseline_cohort.get_stats("age")
  cohort_age_distr = baseline_cohort.get_distributions('age')
  ```
  Note that currently the `get_stats()` method only returns statistics of age, gender, race, and ethinicity of a cohort 
and `get_distributions()` method only returns distribution of age and gender in a cohort.
- You can also explore concept prevalence within a cohort - a key step in identifying potential biases during 
cohort selection. A concept refers to a coded term from a standardized medical vocabulary, uniquely identified by a 
concept ID. All clinical events in OMOP, such as conditions, drug exposures, procedures, measurements, and events, are 
represented as concepts. You can get patient counts and prevalence associated with each concept by accessing 
the method `get_concept_stats()` with a code snippet example shown below.
  ```angular2html
    cohort_concepts, cohort_concept_hierarchy = baseline_cohort_data.get_concept_stats(concept_type='condition_occurrence')
    print(pd.DataFrame(cohort_concepts["condition_occurrence"]))
    print(f"returned cohort_concept_hierarchy object converted to dict: {cohort_concept_hierarchy.to_dict()}")
  ```
  The returned cohort_concept_hierarchy object stores concept hierarchical relationsips with concept nodes indexed 
to allow quick information retrival of a concept node and provides hierarchy traversal methods for concept hierarchy 
navigation. For more details, refer to the corresponding tutorial notebook [BiasAnalyzerCohortConceptTutorial.ipynb](https://github.com/VACLab/BiasAnalyzer/blob/main/notebooks/BiasAnalyzerCohortConceptTutorial.ipynb).
- There is also an API method `get_cohorts_concept_stats(list_of_cohort_ids, concept_type='condition_occurrence', filter_count=0, vocab=None)` 
that enables users to explore union of concept prevalences over multiple cohorts to facilitate potential cohort 
selection bias exploration. An example code snippet is shown below to illustrate how to use this method.
   ```angular2html
   cohort_list = [baseline_cohort_data.cohort_id, study_cohort_data.cohort_id]
   aggregated_cohort_metrics_dict = bias.get_cohorts_concept_stats(cohort_list)
   print('Aggregated concept prevalence metrics over the baseline and study cohorts are:')
   print(aggregated_cohort_metrics_dict)
   ```
- There is also an API method that enables users to compare distributions of two cohorts by calling `bias.compare_cohorts(cohort1_id, cohort2_id)` 
where cohort1_id and cohort2_id are integers and can be obtained from metadata of a cohort object. Currently, 
only hellinger distances between distributions of two cohorts are computed.

- After all analysis is done, please make sure to close database connections and do necessary cleanups by calling 
the API method `bias.cleanup()`.

---

## 📘 Tutorial Notebooks

To help users get started with the `BiasAnalyzer` python package, four Jupyter notebooks are 
provided in the [`notebooks/`](https://github.com/VACLab/BiasAnalyzer/tree/main/notebooks) 
directory. These tutorials walk users through key features and workflows with illustrative examples.

| Tutorial | Description                                                                                                                                                                                                           |
|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [BiasAnalyzerCohortsTutorial.ipynb](https://github.com/VACLab/BiasAnalyzer/blob/main/notebooks/BiasAnalyzerCohortsTutorial.ipynb) | Demonstrates how to create baseline and study cohorts, retrieve cohort statistics, and compare cohort distributions.                                                                                                  |
| [BiasAnalyzerAsyncCohortsTutorial.ipynb](https://github.com/VACLab/BiasAnalyzer/blob/main/notebooks/BiasAnalyzerAsyncCohortsTutorial.ipynb) | As a companion to the Cohort tutorial above, demonstrates how to create and analyze cohorts asynchronously for improved performance and responsiveness when working with large datasets or complex cohort definitions. |
| [BiasAnalyzerCohortConceptTutorial.ipynb](https://github.com/VACLab/BiasAnalyzer/blob/main/notebooks/BiasAnalyzerCohortConceptTutorial.ipynb) | Demonstrates how to explore clinical concept prevalence within a cohort, helping users analyze clinical concept prevalence and identify potential cohort selection biases.                                            |
| [BiasAnalyzerConceptBrowsingTutorial.ipynb](https://github.com/VACLab/BiasAnalyzer/blob/main/notebooks/BiasAnalyzerConceptBrowsingTutorial.ipynb) | Guides users through browsing OMOP concepts, domains, and vocabularies, including how to retrieve and visualize concept hierarchies.                                                                                  |

These tutorials are designed to run in a Jupyter environment with access to an OMOP-compatible postgreSQL or DuckDB database. 

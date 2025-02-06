[![test](https://github.com/VACLab/BiasAnalyzer/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/VACLab/BiasAnalyzer/actions/workflows/test.yml)
<h1>
  <img src="./assets/logo.png" alt="Project Logo" width="50" style="vertical-align: middle; margin-right: 10px;">
  BiasAnalyzer (BA)
</h1>

BiasAnalyzer is a Python package that enables users to track, quantify, analyze, and communicate bias in cohort 
selection. In this context, bias is viewed as lack of representation within a cohort compared to a baseline population.

To import the BiasAnalyzer package and use the BIAS API, you can follow the steps outlined below. Sample commands or 
code snippets are also provided in each step for easy reference.

- Install the BiasAnalyzer python package from [pypi](https://pypi.org/) or 
install it directly from this github repo. For example, run 
`!TMPDIR=<local_temp_dir> pip install git+https://github.com/vaclab/BiasAnalyzer.git --target <local_target_dir> --upgrade`
in a Jupyter notebook to install the python package from this github repo. 
- Run code below to append the target directory where the package was installed to PYTHONPATH.
  ```
  import sys
  sys.path.append('/home/hyi/temp')
  ```
- Run `from biasanalyzer.api import BIAS` to import the module API.
- Run `bias = BIAS()` to create an object of the imported BIAS class.
- Create a config.yaml file for specifying OMOP database connection configuration information. 
The config.yaml file must include root_omop_cdm_database key. 
- [A test OMOP database configuration yaml file](https://github.com/VACLab/BiasAnalyzer/blob/main/tests/assets/test_config.yaml) 
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
(e.g., call `bias.display_concept_tree(parent_concept_tree,  show_in_text_format=False)`)to leverage the tree widget for displaying 
the hierarchy in a tree that can be expanded and collapsed on demand interactively.   

In addition, you can create a cohort 
- Run code snippets below to create a baseline patient cohort.
  ```angular2html
     
  ```




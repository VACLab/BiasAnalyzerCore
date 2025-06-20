import time
from pydantic import ValidationError
from biasanalyzer.database import OMOPCDMDatabase, BiasDatabase
from biasanalyzer.cohort import CohortAction
from biasanalyzer.config import load_config
from ipywidgets import VBox, Label
from ipytree import Tree
from IPython.display import display
from biasanalyzer.utils import get_direction_arrow, notify_users, build_concept_tree


class BIAS:
    def __init__(self, config_file_path=None):
        self.bias_db = None
        self.omop_cdm_db = None
        self.cohort_action = None
        if config_file_path is None:
            self.config = {}
        else:
            self.set_config(config_file_path)

    def set_config(self, config_file_path: str):
        if not config_file_path:
            notify_users('no configuration file specified. '
                         'Call set_config(config_file_path) next to specify configurations')
        else:
            try:
                self.config = load_config(config_file_path)
                notify_users(f'configuration specified in {config_file_path} loaded successfully')
            except FileNotFoundError:
                notify_users('specified configuration file does not exist. '
                             'Call set_config(config_file_path) next to specify a valid configuration file',
                             level='error')
            except ValidationError as ex:
                notify_users(f'configuration yaml file is not valid with validation error: {ex}', level='error')

    def set_root_omop(self):
        if not self.config:
            notify_users('no valid configuration to set root OMOP CDM data. '
                         'Call set_config(config_file_path) to specify configurations first.')
        else:
            db_type = self.config['root_omop_cdm_database']['database_type']
            if db_type == 'postgresql':
                user = self.config['root_omop_cdm_database']['username']
                password = self.config['root_omop_cdm_database']['password']
                host = self.config['root_omop_cdm_database']['hostname']
                port = self.config['root_omop_cdm_database']['port']
                db = self.config['root_omop_cdm_database']['database']
                db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
                self.omop_cdm_db = OMOPCDMDatabase(db_url)
                self.bias_db = BiasDatabase(':memory:')
                # load postgres extension in duckdb bias_db so that cohorts in duckdb can be joined
                # with OMOP CDM tables in omop_cdm_db
                self.bias_db.load_postgres_extension()
                self.bias_db.omop_cdm_db_url = db_url

            elif db_type == 'duckdb':
                db_path = self.config['root_omop_cdm_database'].get('database', ":memory:")
                self.omop_cdm_db = OMOPCDMDatabase(db_path)
                self.bias_db = BiasDatabase(db_path)
                self.bias_db.omop_cdm_db_url = db_path
            else:
                notify_users(f"Unsupported database type: {db_type}")

    def _set_cohort_action(self):
        if self.omop_cdm_db is None:
            notify_users('A valid OMOP CDM must be set before creating a cohort. '
                         'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        if self.cohort_action is None:
            self.cohort_action = CohortAction(self.omop_cdm_db, self.bias_db)
        return self.cohort_action

    def get_domains_and_vocabularies(self):
        if self.omop_cdm_db is None:
            notify_users('A valid OMOP CDM must be set before getting domains. '
                         'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        return self.omop_cdm_db.get_domains_and_vocabularies()

    def get_concepts(self, search_term, domain=None, vocabulary=None):
        if self.omop_cdm_db is None:
            notify_users('A valid OMOP CDM must be set before getting concepts. '
                         'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        if domain is None and vocabulary is None:
            notify_users('either domain or vocabulary must be set to constrain the number of returned concepts')
            return None
        return self.omop_cdm_db.get_concepts(search_term, domain, vocabulary)

    def get_concept_hierarchy(self, concept_id):
        if self.omop_cdm_db is None:
            notify_users('A valid OMOP CDM must be set before getting concepts. '
                         'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        return self.omop_cdm_db.get_concept_hierarchy(concept_id)

    def display_concept_tree(self, concept_tree: dict, level: int = 0, show_in_text_format=True):
        """
        Recursively prints the concept hierarchy tree in an indented format for display.
        """
        details = concept_tree.get("details", {})
        if 'parents' in concept_tree:
            tree_type = 'parents'
        elif 'children' in concept_tree:
            tree_type = 'children'
        else:
            notify_users('The input concept tree must contain parents or children key as the type of the tree.')
            return ''

        if show_in_text_format:
            if details:
                direction_arrow = get_direction_arrow(tree_type)
                print(
                    "  " * level + f"{direction_arrow} {details['concept_name']} (ID: {details['concept_id']}, "
                                   f"Code: {details['concept_code']})")

            for child in concept_tree.get(tree_type, []):
                if child:
                    self.display_concept_tree(child, level + 1, show_in_text_format=True)
            # return empty string to print None being printed at the end of printout
            return ""
        else:
            # Extract concept details
            # Build the root tree node
            root_node = build_concept_tree(concept_tree, tree_type)
            tree = Tree()
            tree.add_node(root_node)
            tree.opened = True
            display(VBox([Label("Concept Hierarchy"), tree]))
            return root_node


    def create_cohort(self, cohort_name: str, cohort_desc: str, query_or_yaml_file: str, created_by: str,
                      delay: float=0):
        """
        API method that allows to create a cohort
        :param cohort_name: name of the cohort
        :param cohort_desc: description of the cohort
        :param query_or_yaml_file: a SQL query or YAML cohort creation file
        :param created_by: name of the user that created the cohort
        :param delay: the number of seconds to sleep/delay for simulating long-running task for async testing,
        default is 0, meaning no delay
        :return: CohortData object if cohort is created successfully; otherwise, None
        """

        c_action = self._set_cohort_action()
        if c_action:
            created_cohort = c_action.create_cohort(cohort_name, cohort_desc, query_or_yaml_file, created_by)
            if created_cohort is not None:
                if delay > 0:
                    notify_users(f"[DEBUG] Simulating long-running task with {delay} seconds delay...")
                    time.sleep(delay)
                notify_users('cohort created successfully')
            return created_cohort
        else:
            notify_users('failed to create a valid cohort action object')
            return None


    def compare_cohorts(self, cohort_id1, cohort_id2):
        c_action = self._set_cohort_action()
        if c_action:
            return c_action.compare_cohorts(cohort_id1, cohort_id2)
        else:
            notify_users('failed to create a valid cohort action object')
            return None


    def cleanup(self):
        if self.bias_db:
            self.bias_db.close()
        if self.omop_cdm_db:
            self.omop_cdm_db.close()
        if self.cohort_action:
            del self.cohort_action

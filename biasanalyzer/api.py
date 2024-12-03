from pydantic import ValidationError
from biasanalyzer.database import OMOPCDMDatabase, BiasDatabase
from biasanalyzer.cohort import CohortAction
from biasanalyzer.config import load_config
from ipywidgets import VBox, Label
from ipytree import Tree, Node
from IPython.display import display
from biasanalyzer.utils import get_direction_arrow


class BIAS:
    _instance = None

    def __init__(self):
        self.config = {}
        self.bias_db = None
        self.omop_cdm_db = None
        self.cohort_action = None

    def __new__(cls, config_file_path=None):
        if cls._instance is None:
            cls._instance = super(BIAS, cls).__new__(cls)
            cls._instance.set_config(config_file_path)
        return cls._instance

    def set_config(self, config_file_path: str):
        if config_file_path is None:
            print('no configuration file specified. '
                  'Call set_config(config_file_path) next to specify configurations')
        else:
            try:
                self.config = load_config(config_file_path)
                print(f'configuration specified in {config_file_path} loaded successfully')
            except FileNotFoundError:
                print('specified configuration file does not exist. '
                      'Call set_config(config_file_path) next to specify a valid '
                      'configuration file')
            except ValidationError as ex:
                print(f'configuration yaml file is not valid with validation error: {ex}')

    def set_root_omop(self):
        if not self.config:
            print('no valid configuration to set root OMOP CDM data. '
                  'Call set_config(config_file_path) to specify configurations first.')
        elif 'root_omop_cdm_database' in self.config:
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
                print(f"Unsupported database type: {db_type}")
        else:
            print('Configuration file must include configuration values for root_omop_cdm_database key.')

    def _set_cohort_action(self):
        if self.omop_cdm_db is None:
            print('A valid OMOP CDM must be set before creating a cohort. '
                  'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        if self.cohort_action is None:
            self.cohort_action = CohortAction(self.omop_cdm_db, self.bias_db)
        return self.cohort_action

    def get_domains_and_vocabularies(self):
        if self.omop_cdm_db is None:
            print('A valid OMOP CDM must be set before getting domains. '
                  'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        return self.omop_cdm_db.get_domains_and_vocabularies()

    def get_concepts(self, search_term, domain=None, vocabulary=None):
        if self.omop_cdm_db is None:
            print('A valid OMOP CDM must be set before getting concepts. '
                  'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        if domain is None and vocabulary is None:
            print('either domain or vocabulary must be set to constrain the number of returned concepts')
            return None
        return self.omop_cdm_db.get_concepts(search_term, domain, vocabulary)

    def get_concept_hierarchy(self, concept_id):
        if self.omop_cdm_db is None:
            print('A valid OMOP CDM must be set before getting concepts. '
                  'Call set_root_omop first to set a valid root OMOP CDM')
            return None
        return self.omop_cdm_db.get_concept_hierarchy(concept_id)

    def _build_concept_tree(self, concept_tree: dict, tree_type: str) -> Node:
        """
            Recursively builds an ipytree Node for a given concept tree.
            """
        # Extract concept details
        details = concept_tree.get("details", {})
        concept_name = details.get("concept_name", "Unknown Concept")
        concept_id = details.get("concept_id", "")
        concept_code = details.get("concept_code", "")
        direction_arrow = get_direction_arrow(tree_type)
        # Create a label for the current concept
        label_text = f"{direction_arrow} {concept_name} (ID: {concept_id}, Code: {concept_code})"
        node = Node(label_text)

        # Recursively add child nodes
        for child in concept_tree.get(tree_type, []):
            child_node = self._build_concept_tree(child, tree_type)
            node.add_node(child_node)

        return node

    def display_concept_tree(self, concept_tree: dict, level: int = 0, show_in_text_format=True, tree_type=None):
        """
        Recursively prints the concept hierarchy tree in an indented format for display.
        """
        details = concept_tree.get("details", {})
        if tree_type is None or tree_type not in ['parents', 'children']:
            if 'parents' in concept_tree:
                tree_type = 'parents'
            elif 'children' in concept_tree:
                tree_type = 'children'
            else:
                print('The input concept tree must contain parents or children key as the type of the tree.')
                return ''

        if show_in_text_format:
            if details:
                direction_arrow = get_direction_arrow(tree_type)
                print(
                    "  " * level + f"{direction_arrow} {details['concept_name']} (ID: {details['concept_id']}, "
                                   f"Code: {details['concept_code']})")

            for child in concept_tree.get(tree_type, []):
                if child:
                    self.display_concept_tree(child, level + 1, tree_type=tree_type, show_in_text_format=True)
            # return empty string to print None being printed at the end of printout
            return ""
        else:
            # Extract concept details
            # Build the root tree node
            root_node = self._build_concept_tree(concept_tree, tree_type)
            tree = Tree()
            tree.add_node(root_node)
            tree.opened = True
            display(VBox([Label("Concept Hierarchy"), tree]))
            return None


    def create_cohort(self, cohort_name, cohort_desc, query, created_by):
        c_action = self._set_cohort_action()
        if c_action:
            created_cohort = c_action.create_cohort(cohort_name, cohort_desc, query, created_by)
            print('cohort created successfully')
            return created_cohort
        else:
            print('failed to create a valid cohort action object')
            return None

    def compare_cohorts(self, cohort_id1, cohort_id2):
        c_action = self._set_cohort_action()
        if c_action:
            return c_action.compare_cohorts(cohort_id1, cohort_id2)
        else:
            print('failed to create a valid cohort action object')

    def cleanup(self):
        self.bias_db.close()
        self.omop_cdm_db.close()
        del self.cohort_action

import duckdb
import pandas as pd
from typing import Optional
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
from biasanalyzer.models import Cohort, CohortDefinition
from biasanalyzer.sql import *
from biasanalyzer.utils import build_concept_hierarchy, print_hierarchy, find_roots


class BiasDatabase:
    distribution_queries = {
        "age": AGE_DISTRIBUTION_QUERY,
        "gender": GENDER_DISTRIBUTION_QUERY,
    }
    stats_queries = {
        "age": AGE_STATS_QUERY,
        "gender": GENDER_STATS_QUERY,
        "race": RACE_STATS_QUERY,
        "ethnicity": ETHNICITY_STATS_QUERY
    }
    cohort_concept_queries = {
        'condition_occurrence': {
            'query': COHORT_CONCEPT_CONDITION_PREVALENCE_QUERY,
            'default_vocab': 'SNOMED'
        },
        'drug_exposure': {
            'query': COHORT_CONCEPT_DRUG_PREVALENCE_QUERY,
            'default_vocab': 'RxNorm'
        }
    }
    _instance = None  # indicating a singleton with only one instance of the class ever created
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(BiasDatabase, cls).__new__(cls)
            cls._instance._initialize(*args, **kwargs)  # Initialize only once
        return cls._instance

    def _initialize(self, db_url):
        # by default, duckdb uses in memory database
        self.conn = duckdb.connect(db_url)
        self.omop_cdm_db_url = None
        self._create_cohort_definition_table()
        self._create_cohort_table()

    def _create_cohort_definition_table(self):
        try:
            self.conn.execute('CREATE SEQUENCE id_sequence START 1')
        except duckdb.Error as e:
            if "already exists" in str(e).lower():
                print("Sequence already exists, skipping creation.")
            else:
                raise
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cohort_definition (
                      id INTEGER DEFAULT nextval('id_sequence'), 
                      name VARCHAR NOT NULL, 
                      description VARCHAR, 
                      created_date DATE, 
                      creation_info VARCHAR, 
                      created_by VARCHAR,
                      PRIMARY KEY (id)
                      )
                ''')
        print("Cohort Definition table created.")

    def _create_cohort_table(self):
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cohort (
                subject_id BIGINT,
                cohort_definition_id INTEGER,
                cohort_start_date DATE,
                cohort_end_date DATE,
                FOREIGN KEY (cohort_definition_id) REFERENCES cohort_definition(id)
            )
        ''')
        try:
            self.conn.execute('''
                CREATE INDEX idx_cohort_dates ON cohort (cohort_definition_id, cohort_start_date, cohort_end_date);
            ''')
        except duckdb.Error as e:
            if "already exists" in str(e).lower():
                print("Index already exists, skipping creation.")
            else:
                raise
        print("Cohort table created.")

    def load_postgres_extension(self):
        self.conn.execute("INSTALL postgres_scanner;")
        self.conn.execute("LOAD postgres_scanner;")

    def create_cohort_definition(self, cohort_definition: CohortDefinition):
        self.conn.execute('''
            INSERT INTO cohort_definition (name, description, created_date, creation_info, created_by)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            cohort_definition.name,
            cohort_definition.description,
            cohort_definition.created_date or datetime.now(),
            cohort_definition.creation_info,
            cohort_definition.created_by
        ))
        print("Cohort definition inserted successfully.")
        self.conn.execute("SELECT id from cohort_definition ORDER BY id DESC LIMIT 1")
        created_cohort_id = self.conn.fetchone()[0]
        return created_cohort_id

    # Method to insert cohort data
    def create_cohort(self, cohort: Cohort):
        self.conn.execute('''
            INSERT INTO cohort (subject_id, cohort_definition_id, cohort_start_date, cohort_end_date)
            VALUES (?, ?, ?, ?)
        ''', (
            cohort.subject_id,
            cohort.cohort_definition_id,
            cohort.cohort_start_date,
            cohort.cohort_end_date
        ))

    # Method to insert cohort data in bulk from a dataframe
    def create_cohort_in_bulk(self, cohort_df: pd.DataFrame):
        # make duckdb to treat cohort_df dataframe as a virtual table named "cohort_df"
        self.conn.register("cohort_df", cohort_df)
        self.conn.execute('''
            INSERT INTO cohort (subject_id, cohort_definition_id, cohort_start_date, cohort_end_date)
            SELECT subject_id, cohort_definition_id, cohort_start_date, cohort_end_date FROM cohort_df
        ''')

    def get_cohort_definition(self, cohort_definition_id):
        results = self.conn.execute(f'''
        SELECT id, name, description, created_date, creation_info, created_by FROM cohort_definition 
        WHERE id = {cohort_definition_id} 
        ''')
        headers = [desc[0] for desc in results.description]
        row = results.fetchall()
        if len(row) == 0:
            return {}
        else:
            return dict(zip(headers, row[0]))

    def get_cohort(self, cohort_definition_id):
        results = self.conn.execute(f'''
        SELECT subject_id, cohort_definition_id, cohort_start_date, cohort_end_date FROM cohort 
        WHERE cohort_definition_id = {cohort_definition_id}
        ''')
        headers = [desc[0] for desc in results.description]
        rows = results.fetchall()
        return [dict(zip(headers, row)) for row in rows]

    def _create_omop_table(self, table_name):
        if self.omop_cdm_db_url is not None and not self.omop_cdm_db_url.endswith('.duckdb'):
            # need to create person table from OMOP CDM postgreSQL database
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * from postgres_scan('{self.omop_cdm_db_url}', 'public', {table_name})
            """)
            return True # success
        elif self.omop_cdm_db_url.endswith('.duckdb'):
            return True
        else:
            return False # failure

    def _execute_query(self, query_str):
        results = self.conn.execute(query_str)

        headers = [desc[0] for desc in results.description]
        rows = results.fetchall()
        if len(rows) == 0:
            return []
        else:
            return [dict(zip(headers, row)) for row in rows]

    def get_cohort_basic_stats(self, cohort_definition_id: int, variable=''):
        """
        Get aggregation statistics for a cohort from the cohort table.
        :param cohort_definition_id: cohort definition id representing the cohort
        :param variable: optional with an empty string as default. If empty, basic stats of
        the cohort are returned; If set to a specific variable such as age, gender, race,
        the stats of the specified variable in the cohort are returned
        :return: cohort stats corresponding to the specified variable
        """
        try:
            if variable:
                if self._create_omop_table('person'):
                    query_str = self.__class__.stats_queries.get(variable)
                    if query_str is None:
                        raise ValueError(f"Statistics for variable '{variable}' is not available. "
                                         f"Valid variables are {self.__class__.stats_queries.keys()}")
                    stats_query = query_str.format(cohort_definition_id)
                else:
                    print("Cannot connect to the OMOP database to query person table")
                    return None
            else:
                # Query the cohort data to get basic statistics
                stats_query = f'''
                    WITH cohort_Duration AS (
                        SELECT
                            subject_id,
                            cohort_start_date,
                            cohort_end_date,
                            cohort_end_date - cohort_start_date AS duration_days
                        FROM
                            cohort
                        WHERE cohort_definition_id = {cohort_definition_id}    
                    )
                    SELECT
                        COUNT(*) AS total_count,
                        MIN(cohort_start_date) AS earliest_start_date,
                        MAX(cohort_start_date) AS latest_start_date,
                        MIN(cohort_end_date) AS earliest_end_date,
                        MAX(cohort_end_date) AS latest_end_date,
                        MIN(duration_days) AS min_duration_days,
                        MAX(duration_days) AS max_duration_days,
                        ROUND(AVG(duration_days), 2) AS avg_duration_days,
                        CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_days) AS INT) AS median_duration,
                        ROUND(STDDEV(duration_days), 2) AS stddev_duration
                    FROM cohort_Duration                    
                '''
            return self._execute_query(stats_query)

        except Exception as e:
            print(f"Error computing cohort basic statistics: {e}")
            return None

    @property
    def cohort_distribution_variables(self):
        return self.__class__.distribution_queries.keys()

    def get_cohort_distributions(self, cohort_definition_id: int, variable: str):
        """
        Get age distribution statistics for a cohort from the cohort table.
        """
        try:
            if self._create_omop_table('person'):
                query_str = self.__class__.distribution_queries.get(variable)
                if query_str is None:
                    raise ValueError(f"Distribution for variable '{variable}' is not available. "
                                     f"Valid variables are {self.__class__.distribution_queries.keys()}")
                query = query_str.format(cohort_definition_id)
                return self._execute_query(query)
            else:
                print("Cannot connect to the OMOP database to query person table")
                return None
        except Exception as e:
            print(f"Error computing cohort {variable} distributions: {e}")
            return None

    def get_cohort_concept_stats(self, cohort_definition_id: int,
                                 concept_type='condition_occurrence', filter_count=0, vocab=None,
                                 include_hierarchy=False):
        """
        Get concept statistics for a cohort from the cohort table.
        """
        concept_stats = {}
        if concept_type not in self.__class__.cohort_concept_queries:
            print(f"input {concept_type} is not a valid concept type. "
                  f"Supported concept types are: {self.__class__.cohort_concept_queries.keys()}")
            return concept_stats
        try:
            if self._create_omop_table('concept') and self._create_omop_table('concept_ancestor'):
                query_str = self.__class__.cohort_concept_queries[concept_type]['query']
                if self._create_omop_table(concept_type):
                    if not vocab:
                        vocab = self.__class__.cohort_concept_queries[concept_type]['default_vocab']
                    query = query_str.format(cid=cohort_definition_id, filter_count=filter_count,
                                             vocab=vocab, include_hierarchy=include_hierarchy)
                    concept_stats[concept_type] = self._execute_query(query)
                    cs_df = pd.DataFrame(concept_stats[concept_type])
                    # Combine concept_name and prevalence into a "details" column
                    cs_df["details"] = cs_df.apply(
                        lambda row: f"{row['concept_name']} (Code: {row['concept_code']}, "
                                    f"Count: {row['count_in_cohort']}, Prevalence: {row['prevalence']:.3%})", axis=1)
                    filtered_cs_df = cs_df[cs_df['ancestor_concept_id'] != cs_df['descendant_concept_id']]
                    roots = find_roots(filtered_cs_df)
                    hierarchy = build_concept_hierarchy(filtered_cs_df)
                    print(f'cohort concept hierarchy for {concept_type} with root concept ids {roots}:')
                    for root in roots:
                        root_detail = cs_df[(cs_df['ancestor_concept_id'] == root)
                                  & (cs_df['descendant_concept_id'] == root)]['details'].iloc[0]
                        print_hierarchy(hierarchy, parent=root, level=0, parent_details=root_detail)
                    return concept_stats
                else:
                    print(f"Cannot connect to the OMOP database to query {concept_type} table")
                    return concept_stats
            else:
                print("Cannot connect to the OMOP database to query concept table")
                return concept_stats
        except Exception as e:
            print(f"Error computing cohort concept stats: {e}")
            return concept_stats

    def close(self):
        self.conn.close()
        BiasDatabase._instance = None
        print("Connection to BiasDatabase closed.")


class OMOPCDMDatabase:
    _instance = None  # indicating a singleton with only one instance of the class ever created
    _database_type = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(OMOPCDMDatabase, cls).__new__(cls)
            cls._instance._initialize(*args, **kwargs)  # Initialize only once
        return cls._instance

    def _initialize(self, db_url):
        if db_url.endswith('.duckdb'):
            # Handle DuckDB connection
            try:
                self.engine = duckdb.connect(db_url)
                print(f"Connected to the DuckDB database: {db_url}.")
            except duckdb.Error as e:
                print(f"Failed to connect to DuckDB: {e}")
            self.Session = self.engine  # Use engine directly for DuckDB
            self._database_type = 'duckdb'
        try:
            self.engine = create_engine(
                db_url,
                echo=False,
                connect_args={'options': '-c default_transaction_read_only=on'}  # Enforce read-only transactions
            )
            self.Session = sessionmaker(bind=self.engine)
            print("Connected to the OMOP CDM database (read-only).")
            self._database_type = 'postgresql'
        except SQLAlchemyError as e:
            print(f"Failed to connect to the database: {e}")

    def get_session(self):
        if self._database_type == 'duckdb':
            return self.engine
        else: # postgresql connection
            # Provide a new session for read-only queries
            return self.Session()

    def execute_query(self, query, params=None):
        try:
            if self._database_type == 'duckdb':
                # DuckDB query execution
                results = self.engine.execute(query, params).fetchall()
                headers = [desc[0] for desc in self.engine.execute(query, params).description]
            else:
                # PostgreSQL query execution
                omop_session = self.get_session()
                query = text(query)
                results = omop_session.execute(query, params) if params else omop_session.execute(query)
                headers = results.keys()
                results = results.fetchall()
                omop_session.close()

            return [dict(zip(headers, row)) for row in results]

        except duckdb.Error as e:
            print(f"Error executing query: {e}")
            return []
        except SQLAlchemyError as e:
            print(f"Error executing query: {e}")
            omop_session.close()
            return []

    def get_domains_and_vocabularies(self) -> list:
        # find a concept ID based on a search term
        query = """
                    SELECT distinct domain_id, vocabulary_id FROM concept order by domain_id, vocabulary_id
                """
        return self.execute_query(query)

    def get_concepts(self, search_term: str, domain: Optional[str], vocab: Optional[str]) -> list:
        # find a concept ID based on a search term
        search_term_exact = search_term.lower()
        search_term_suffix = f'{search_term_exact} '
        search_term_prefix = f' {search_term_exact}'
        search_term_prefix_suffix = f' {search_term_exact} '
        param_set = {
            "search_term_exact": search_term_exact,
            "search_term_prefix": search_term_prefix,
            "search_term_suffix": search_term_suffix,
            "search_term_prefix_suffix": search_term_prefix_suffix
        }
        if domain is not None and vocab is not None:
            condition_str = "domain_id = :domain and vocabulary_id = :vocabulary"
            param_set['domain'] = domain
            param_set['vocabulary'] = vocab
        elif domain is None:
            condition_str = "vocabulary_id = :vocabulary"
            param_set['vocabulary'] = vocab
        else:
            # vocab is None
            condition_str = "domain_id = :domain"
            param_set['domain'] = domain

        query = f"""
        SELECT concept_id, concept_name, valid_start_date, valid_end_date, domain_id, vocabulary_id FROM concept 
        where {condition_str} and 
        (LOWER(concept_name) = :search_term_exact or LOWER(concept_name) LIKE '%' || :search_term_prefix
        or LOWER(concept_name) LIKE :search_term_suffix || '%'
        or LOWER(concept_name) LIKE '%' || :search_term_prefix_suffix || '%')
        ORDER BY concept_id
        """

        return self.execute_query(query, params=param_set)

    def get_concept_hierarchy(self, concept_id: int):
        """
        Retrieves the full concept hierarchy (ancestors and descendants) for a given concept_id
        and organizes it into a nested dictionary to represent the tree structure.
        """
        query = """
                WITH RECURSIVE concept_hierarchy AS (
                    SELECT ancestor_concept_id, descendant_concept_id, min_levels_of_separation
                    FROM concept_ancestor
                    WHERE ancestor_concept_id = :concept_id OR descendant_concept_id = :concept_id

                    UNION

                    SELECT ca.ancestor_concept_id, ca.descendant_concept_id, ca.min_levels_of_separation
                    FROM concept_ancestor ca
                    JOIN concept_hierarchy ch ON ca.ancestor_concept_id = ch.descendant_concept_id
                )
                SELECT ancestor_concept_id, descendant_concept_id
                FROM concept_hierarchy
                WHERE min_levels_of_separation > 0
                """

        results = self.execute_query(query, params={"concept_id": concept_id})

        # Collect all unique concept IDs involved in the hierarchy using set comprehension
        concept_ids = {row['ancestor_concept_id'] for row in results} | {row['descendant_concept_id'] for row in results}
        # Fetch details of each concept
        concept_details = {}
        if concept_ids:
            query = """
                    SELECT concept_id, concept_name, vocabulary_id, concept_code
                    FROM concept
                    WHERE concept_id IN :concept_ids
                    """

            result = self.execute_query(query, params={"concept_ids": tuple(concept_ids)})
            concept_details = {row['concept_id']: row for row in result}

        # Build the hierarchy tree using a dictionary
        hierarchy = {}
        reverse_hierarchy = {}
        for row in results:
            ancestor_id = row['ancestor_concept_id']
            descendant_id = row['descendant_concept_id']

            if ancestor_id not in hierarchy:
                hierarchy[ancestor_id] = {"details": concept_details[ancestor_id], "children": []}
            if descendant_id not in hierarchy:
                hierarchy[descendant_id] = {"details": concept_details[descendant_id], "children": []}
            # Link descendants to their ancestor node
            hierarchy[ancestor_id]["children"].append(hierarchy[descendant_id])

            if descendant_id not in reverse_hierarchy:
                reverse_hierarchy[descendant_id] = {"details": concept_details[descendant_id], "parents": []}
            if ancestor_id not in reverse_hierarchy:
                reverse_hierarchy[ancestor_id] = {"details": concept_details[ancestor_id], "parents": []}
            # Link ancestors to their descendant (child) node
            reverse_hierarchy[descendant_id]["parents"].append(reverse_hierarchy[ancestor_id])

        # Return the parent hierarchy and children hierarchy of the specified concept
        return reverse_hierarchy[concept_id], hierarchy[concept_id]


    def close(self):
        if isinstance(self.engine, duckdb.DuckDBPyConnection):
            self.engine.close()
        else:
            self.engine.dispose()
        OMOPCDMDatabase._instance = None
        print("Connection to the OMOP CDM database closed.")

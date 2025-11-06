# ruff: noqa: S608
import gc
from datetime import datetime
from typing import Optional

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from tqdm.auto import tqdm

from biasanalyzer.models import CohortDefinition
from biasanalyzer.sql import (
    AGE_DISTRIBUTION_QUERY,
    AGE_STATS_QUERY,
    ETHNICITY_STATS_QUERY,
    GENDER_DISTRIBUTION_QUERY,
    GENDER_STATS_QUERY,
    RACE_STATS_QUERY,
)
from biasanalyzer.utils import build_concept_hierarchy, find_roots, notify_users, print_hierarchy


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
    _instance = None  # indicating a singleton with only one instance of the class ever created
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(*args, **kwargs)  # Initialize only once
        return cls._instance

    def _initialize(self, db_url, omop_db_url=None):
        # by default, duckdb uses in memory database
        self.conn = duckdb.connect(db_url)
        self.schema = "biasanalyzer"
        self.omop_alias = 'omop'
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.omop_cdm_db_url = omop_db_url
        if omop_db_url is not None:
            if omop_db_url.startswith('postgresql://'):
                # omop db is postgreSQL
                self.load_postgres_extension()
                self.conn.execute(f"""            
                            ATTACH '{self.omop_cdm_db_url}' as {self.omop_alias} (TYPE postgres)
                            """)
            elif omop_db_url.endswith('.duckdb'):
                self.conn.execute(f"""
                            ATTACH '{self.omop_cdm_db_url}' as {self.omop_alias}
                            """)
            else:
                raise ValueError("Unsupported OMOP database backend")

            # Set self.schema as default schema
            self.conn.execute(f"SET schema '{self.schema}'")

        self._create_cohort_definition_table()
        self._create_cohort_table()

    def _create_cohort_definition_table(self):
        try:
            self.conn.execute(f'CREATE SEQUENCE {self.schema}.id_sequence START 1')
        except duckdb.Error as e:
            if "already exists" in str(e).lower():
                notify_users("Sequence already exists, skipping creation.")
            else:
                raise
        self.conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.schema}.cohort_definition (
                      id INTEGER DEFAULT nextval('{self.schema}.id_sequence'), 
                      name VARCHAR NOT NULL, 
                      description VARCHAR, 
                      created_date DATE, 
                      creation_info VARCHAR, 
                      created_by VARCHAR,
                      PRIMARY KEY (id)
                      )
                ''')
        notify_users("Cohort Definition table created.")

    def _create_cohort_table(self):
        self.conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.schema}.cohort (
                subject_id BIGINT,
                cohort_definition_id INTEGER,
                cohort_start_date DATE,
                cohort_end_date DATE,
                FOREIGN KEY (cohort_definition_id) REFERENCES {self.schema}.cohort_definition(id)
            )
        ''')
        try:
            self.conn.execute(f'''
                CREATE INDEX idx_cohort_dates ON {self.schema}.cohort (cohort_definition_id, cohort_start_date, 
                cohort_end_date);
            ''')
        except duckdb.Error as e:
            if "already exists" in str(e).lower():
                notify_users("Index already exists, skipping creation.")
            else:
                raise
        notify_users("Cohort table created.")

    def load_postgres_extension(self):
        self.conn.execute("INSTALL postgres;")
        self.conn.execute("LOAD postgres;")

    def create_cohort_definition(self, cohort_definition: CohortDefinition, progress_obj=None):
        self.conn.execute(f'''
            INSERT INTO {self.schema}.cohort_definition (name, description, created_date, creation_info, created_by)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            cohort_definition.name,
            cohort_definition.description,
            cohort_definition.created_date or datetime.now(),
            cohort_definition.creation_info,
            cohort_definition.created_by
        ))
        if progress_obj is None:
            notify_users("Cohort definition inserted successfully.")  # pragma: no cover
        else:
            progress_obj.write("Cohort definition inserted successfully.")
        self.conn.execute(f"SELECT id from {self.schema}.cohort_definition ORDER BY id DESC LIMIT 1")
        created_cohort_id = self.conn.fetchone()[0]
        return created_cohort_id

    # Method to insert cohort data in bulk from a dataframe
    def create_cohort_in_bulk(self, cohort_df: pd.DataFrame):
        # make duckdb to treat cohort_df dataframe as a virtual table named "cohort_df"
        self.conn.register("cohort_df", cohort_df)
        self.conn.execute(f'''
            INSERT INTO {self.schema}.cohort (subject_id, cohort_definition_id, cohort_start_date, cohort_end_date)
            SELECT subject_id, cohort_definition_id, cohort_start_date, cohort_end_date FROM cohort_df
        ''')

    def get_cohort_definition(self, cohort_definition_id):
        results = self.conn.execute(f'''
        SELECT id, name, description, created_date, creation_info, created_by FROM {self.schema}.cohort_definition 
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
        SELECT subject_id, cohort_definition_id, cohort_start_date, cohort_end_date FROM {self.schema}.cohort 
        WHERE cohort_definition_id = {cohort_definition_id}
        ''')
        headers = [desc[0] for desc in results.description]
        rows = results.fetchall()
        return [dict(zip(headers, row)) for row in rows]

    def _execute_query(self, query_str):
        results = self.conn.execute(query_str)

        headers = [desc[0] for desc in results.description]
        rows = results.fetchall()
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
                query_str = self.__class__.stats_queries.get(variable)
                if query_str is None:
                    raise ValueError(f"Statistics for variable '{variable}' is not available. "
                                     f"Valid variables are {self.__class__.stats_queries.keys()}")
                stats_query = query_str.format(ba_schema=self.schema, omop=self.omop_alias,
                                               cohort_definition_id=cohort_definition_id)
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
                            {self.schema}.cohort
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
            notify_users(f"Error computing cohort basic statistics: {e}", level='error')
            return None

    @property
    def cohort_distribution_variables(self):
        return self.__class__.distribution_queries.keys()

    def get_cohort_distributions(self, cohort_definition_id: int, variable: str):
        """
        Get distribution statistics for a cohort from the cohort table.
        """
        try:
            query_str = self.__class__.distribution_queries.get(variable)
            if not query_str:
                raise ValueError(f"Distribution for variable '{variable}' is not available. "
                                 f"Valid variables are {self.__class__.distribution_queries.keys()}")
            query = query_str.format(ba_schema=self.schema, omop=self.omop_alias,
                                     cohort_definition_id=cohort_definition_id)
            return self._execute_query(query)
        except Exception as e:
            notify_users(f"Error computing cohort {variable} distributions: {e}", level='error')
            return None

    def get_cohort_concept_stats(self, cohort_definition_id: int, qry_builder,
                                 concept_type='condition_occurrence', filter_count=0, vocab=None,
                                 print_concept_hierarchy=False):
        """
        Get concept statistics for a cohort from the cohort table.
        """
        concept_stats = {}

        try:
            # validate input vocab if it is not None
            if vocab is not None:
                valid_vocabs = self._execute_query(f"SELECT distinct vocabulary_id FROM {self.omop_alias}.concept")
                valid_vocab_ids = [row['vocabulary_id'] for row in valid_vocabs]
                if vocab not in valid_vocab_ids:
                    err_msg = (f"input {vocab} is not a valid vocabulary in OMOP. "
                               f"Supported vocabulary ids are: {valid_vocab_ids}")
                    notify_users(err_msg, level='error')
                    raise ValueError(err_msg)

            query = qry_builder.build_concept_prevalence_query(self.schema, self.omop_alias, concept_type,
                                                               cohort_definition_id, filter_count, vocab)
            concept_stats[concept_type] = self._execute_query(query)
            cs_df = pd.DataFrame(concept_stats[concept_type])
            # Combine concept_name and prevalence into a "details" column
            cs_df["details"] = cs_df.apply(
                lambda row: f"{row['concept_name']} (Code: {row['concept_code']}, "
                            f"Count: {row['count_in_cohort']}, Prevalence: {row['prevalence']:.3%})", axis=1)

            if print_concept_hierarchy:
                filtered_cs_df = cs_df[cs_df['ancestor_concept_id'] != cs_df['descendant_concept_id']]
                roots = find_roots(filtered_cs_df)
                hierarchy = build_concept_hierarchy(filtered_cs_df)
                notify_users(f'cohort concept hierarchy for {concept_type} with root concept ids {roots}:')
                for root in roots:
                    root_detail = cs_df[(cs_df['ancestor_concept_id'] == root)
                              & (cs_df['descendant_concept_id'] == root)]['details'].iloc[0]
                    print_hierarchy(hierarchy, parent=root, level=0, parent_details=root_detail)
            return concept_stats
        except Exception as e:
            raise ValueError("Error computing cohort concept stats") from e

    def close(self):
        if self.conn:
            self.conn.close()
        BiasDatabase._instance = None
        notify_users("Connection to BiasDatabase closed.")


class OMOPCDMDatabase:
    _instance = None  # indicating a singleton with only one instance of the class ever created
    _database_type = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(*args, **kwargs)  # Initialize only once
        return cls._instance

    def _initialize(self, db_url):
        if db_url.endswith('.duckdb'):
            # close any potential global connections if any
            for obj in gc.get_objects(): # pragma: no cover
                if isinstance(obj, duckdb.DuckDBPyConnection):
                    try:
                        obj.close()
                    except Exception as e:
                        notify_users(f'failed to close the lingering duckdb connection before opening a new one: {e}')

            # Handle DuckDB connection
            try:
                self.engine = duckdb.connect(db_url)
                notify_users(f"Connected to the DuckDB database: {db_url}.")
            except duckdb.Error as e:  # pragma: no cover
                notify_users(f"Failed to connect to DuckDB: {e}", level='error')
            self.Session = self.engine  # Use engine directly for DuckDB
            self._database_type = 'duckdb'
        else:  # pragma: no cover
            # Handle PostgreSQL connection
            try:
                self.engine = create_engine(
                    db_url,
                    echo=False,
                    connect_args={'options': '-c default_transaction_read_only=on'}  # Enforce read-only transactions
                )
                self.Session = sessionmaker(bind=self.engine)
                notify_users("Connected to the OMOP CDM database (read-only).")
                self._database_type = 'postgresql'
            except SQLAlchemyError as e:
                notify_users(f"Failed to connect to the database: {e}", level='error')

    def get_session(self):
        if self._database_type == 'duckdb':
            return self.engine
        else:  # pragma: no cover
            # postgresql connection: provide a new session for read-only queries
            return self.Session()

    def execute_query(self, query, params=None):
        try:
            if self._database_type == 'duckdb':
                # DuckDB query execution
                results = self.engine.execute(query, params).fetchall()
                headers = [desc[0] for desc in self.engine.execute(query, params).description]
            else:  # pragma: no cover
                # PostgreSQL query execution
                omop_session = self.get_session()
                query = text(query)
                results = omop_session.execute(query, params) if params else omop_session.execute(query)
                headers = results.keys()
                results = results.fetchall()
                omop_session.close()

            return [dict(zip(headers, row)) for row in results]

        except duckdb.Error as e:
            notify_users(f"Error executing query: {e}", level='error')
            return []
        except SQLAlchemyError as e:  # pragma: no cover
            notify_users(f"Error executing query: {e}", level='error')
            if omop_session:
                omop_session.close()
            return []

    def get_domains_and_vocabularies(self) -> list:
        # find a concept ID based on a search term
        query = """
                    SELECT distinct domain_id, vocabulary_id FROM concept order by domain_id, vocabulary_id
                """
        return self.execute_query(query)

    def get_concepts(self, search_term: str, domain: Optional[str], vocab: Optional[str]) -> list:
        search_term_exact = search_term.lower()
        search_term_suffix = f'{search_term_exact} '
        search_term_prefix = f' {search_term_exact}'
        search_term_prefix_suffix = f' {search_term_exact} '

        if self._database_type == 'duckdb':
            # Use positional parameters and ? as placeholder to meet duckdb syntax requirement
            base_query = """
                         SELECT concept_id, concept_name, valid_start_date, valid_end_date, domain_id, vocabulary_id \
                         FROM concept
                         WHERE {condition_str} \
                           AND (
                             LOWER (concept_name) = ? \
                            OR
                             LOWER (concept_name) LIKE '%' || ? \
                            OR
                             LOWER (concept_name) LIKE ? || '%' \
                            OR
                             LOWER (concept_name) LIKE '%' || ? || '%'
                             )
                         ORDER BY concept_id \
                         """

            if domain is not None and vocab is not None:
                condition_str = "domain_id = ? AND vocabulary_id = ?"
                params = [domain, vocab, search_term_exact, search_term_prefix, search_term_suffix,
                              search_term_prefix_suffix]
            elif domain is None:
                condition_str = "vocabulary_id = ?"
                params = [vocab, search_term_exact, search_term_prefix, search_term_suffix,
                              search_term_prefix_suffix]
            else:
                condition_str = "domain_id = ?"
                params = [domain, search_term_exact, search_term_prefix, search_term_suffix,
                              search_term_prefix_suffix]

        else:  # pragma: no cover
            # Use named parameters with :param_name syntax for SQLAlchemy/PostgreSQL
            base_query = """
                         SELECT concept_id, concept_name, valid_start_date, valid_end_date, domain_id, vocabulary_id \
                         FROM concept
                         WHERE {condition_str} \
                           AND (
                             LOWER (concept_name) = :search_term_exact \
                            OR
                             LOWER (concept_name) LIKE '%' || :search_term_prefix \
                            OR
                             LOWER (concept_name) LIKE :search_term_suffix || '%' \
                            OR
                             LOWER (concept_name) LIKE '%' || :search_term_prefix_suffix || '%'
                             )
                         ORDER BY concept_id \
                         """

            params = {
                "search_term_exact": search_term_exact,
                "search_term_prefix": search_term_prefix,
                "search_term_suffix": search_term_suffix,
                "search_term_prefix_suffix": search_term_prefix_suffix
            }

            if domain is not None and vocab is not None:
                condition_str = "domain_id = :domain AND vocabulary_id = :vocabulary"
                params['domain'] = domain
                params['vocabulary'] = vocab
            elif domain is None:
                condition_str = "vocabulary_id = :vocabulary"
                params['vocabulary'] = vocab
            else:
                condition_str = "domain_id = :domain"
                params['domain'] = domain

        query = base_query.format(condition_str=condition_str)
        return self.execute_query(query, params=params)

    def get_concept_hierarchy(self, concept_id: int):
        """
        Retrieves the full concept hierarchy (ancestors and descendants) for a given concept_id
        and organizes it into a nested dictionary to represent the tree structure.
        """
        if not isinstance(concept_id, int):
            # this check is important to avoid SQL injection risk
            raise ValueError("concept_id must be an integer")

        stages = [
            "Queried concept hierarchy",
            "Fetched concept details",
            "Built hierarchy tree"
        ]
        progress = tqdm(total=len(stages), desc="Concept Hierarchy", unit="stage")

        progress.set_postfix_str(stages[0])
        # Inline the concept_id directly into the query
        query = f"""
                WITH RECURSIVE concept_hierarchy AS (
                    SELECT ancestor_concept_id, descendant_concept_id, min_levels_of_separation
                    FROM concept_ancestor
                    WHERE ancestor_concept_id = {concept_id} OR descendant_concept_id = {concept_id}

                    UNION

                    SELECT ca.ancestor_concept_id, ca.descendant_concept_id, ca.min_levels_of_separation
                    FROM concept_ancestor ca
                    JOIN concept_hierarchy ch ON ca.ancestor_concept_id = ch.descendant_concept_id
                )
                SELECT ancestor_concept_id, descendant_concept_id
                FROM concept_hierarchy
                WHERE min_levels_of_separation > 0
            """
        results = self.execute_query(query)

        progress.update(1)

        progress.set_postfix_str(stages[1])
        # Collect all unique concept IDs involved in the hierarchy using set comprehension
        concept_ids = {row['ancestor_concept_id'] for row in results} | {row['descendant_concept_id']
                                                                         for row in results}
        # Fetch details of each concept
        concept_details = {}
        if concept_ids:
            # Convert set of integers to comma-separated string
            concept_ids_str = ", ".join(str(cid) for cid in concept_ids)
            query = f"""
                    SELECT concept_id, concept_name, vocabulary_id, concept_code
                    FROM concept
                    WHERE concept_id IN ({concept_ids_str})
                    """

            result = self.execute_query(query)
            concept_details = {row['concept_id']: row for row in result}
        progress.update(1)

        progress.set_postfix_str(stages[2])
        # Build the hierarchy tree using a dictionary
        hierarchy = {}
        reverse_hierarchy = {}
        for row in results:
            ancestor_id = row['ancestor_concept_id']
            descendant_id = row['descendant_concept_id']

            ancestor_entry = hierarchy.setdefault(
                ancestor_id, {"details": concept_details[ancestor_id], "children": []})
            descendant_entry = hierarchy.setdefault(
                descendant_id, {"details": concept_details[descendant_id], "children": []})
            ancestor_entry["children"].append(descendant_entry)

            desc_entry_rev = reverse_hierarchy.setdefault(
                descendant_id, {"details": concept_details[descendant_id], "parents": []})
            ancestor_entry_rev = reverse_hierarchy.setdefault(
                ancestor_id, {"details": concept_details[ancestor_id], "parents": []})
            desc_entry_rev["parents"].append(ancestor_entry_rev)
        progress.update(1)
        progress.close()

        # Return the parent hierarchy and children hierarchy of the specified concept
        return reverse_hierarchy[concept_id], hierarchy[concept_id]


    def close(self):
        if isinstance(self.engine, duckdb.DuckDBPyConnection):
            self.engine.close()
        else:
            self.engine.dispose()  # pragma: no cover
        OMOPCDMDatabase._instance = None
        notify_users("Connection to the OMOP CDM database closed.")

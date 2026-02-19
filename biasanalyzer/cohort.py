from datetime import datetime
from functools import reduce
from typing import List

import pandas as pd
from pydantic import ValidationError
from tqdm.auto import tqdm

from biasanalyzer.cohort_query_builder import CohortQueryBuilder
from biasanalyzer.concept import ConceptHierarchy
from biasanalyzer.config import load_cohort_creation_config
from biasanalyzer.database import BiasDatabase, OMOPCDMDatabase
from biasanalyzer.models import DOMAIN_MAPPING, CohortDefinition
from biasanalyzer.utils import clean_string, hellinger_distance, notify_users


class CohortData:
    def __init__(self, cohort_id: int, bias_db: BiasDatabase, omop_db: OMOPCDMDatabase):
        self.cohort_id = cohort_id
        self.bias_db = bias_db
        self.omop_db = omop_db
        self._cohort_data = None  # cache the cohort data
        self._metadata = None
        self.query_builder = CohortQueryBuilder(cohort_creation=False)

    @property
    def data(self):
        """
        query the database to get the cohort data using cohort_id. Return cached data if already fetched
        :return: cohort data
        """
        if self._cohort_data is None:
            self._cohort_data = self.bias_db.get_cohort(self.cohort_id)
        return self._cohort_data

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self.bias_db.get_cohort_definition(self.cohort_id)
        return self._metadata

    def get_stats(self, variable=""):
        """
        Get aggregation statistics for the cohort in BiasDatabase.
        variable is optional with a default empty string. Supported variables are: age, gender,
        race, and ethnicity.
        """
        return self.bias_db.get_cohort_basic_stats(self.cohort_id, variable=variable)

    def get_distributions(self, variable):
        """
        Get distribution statistics for a variable (e.g., age) in a specific cohort in BiasDatabase.
        """
        return self.bias_db.get_cohort_distributions(self.cohort_id, variable)

    def get_concept_stats(
        self, concept_type="condition_occurrence", filter_count=0, vocab=None, print_concept_hierarchy=False
    ):
        """
        Get cohort concept statistics such as concept prevalence
        """
        if concept_type not in DOMAIN_MAPPING:
            raise ValueError(f"input concept_type {concept_type} is not a valid concept type to get concept stats")

        cohort_stats = self.bias_db.get_cohort_concept_stats(
            self.cohort_id,
            self.query_builder,
            concept_type=concept_type,
            filter_count=filter_count,
            vocab=vocab,
            print_concept_hierarchy=print_concept_hierarchy,
        )
        return (
            cohort_stats,
            ConceptHierarchy.build_concept_hierarchy_from_results(
                self.cohort_id, concept_type, cohort_stats[concept_type], filter_count=filter_count, vocab=vocab
            ),
        )

    def __del__(self):
        self._cohort_data = None
        self._metadata = None


class CohortAction:
    def __init__(self, omop_db: OMOPCDMDatabase, bias_db: BiasDatabase):
        self.omop_db = omop_db
        self.bias_db = bias_db
        self._query_builder = CohortQueryBuilder()

    def create_cohort(self, cohort_name: str, description: str, query_or_yaml_file: str, created_by: str):
        """
        Create a new cohort by executing a query on OMOP CDM database
        and storing the result in BiasDatabase. The query can be passed in directly
        or built from a yaml file using a corresponding SQL query template
        :param cohort_name: the name of the cohort
        :param description: the description of the cohort
        :param query_or_yaml_file: the SQL query string or yaml file name for creating a cohort
        :param created_by: created_by string indicating who created the cohort, it could be 'system',
        or a username, or whatever metadata to record who created the cohort
        :return: CohortData object if cohort is created successfully; otherwise, return None
        """
        stages = [
            "Built query",
            "Executed query on OMOP database to get cohort data",
            "Inserted cohort data into DuckDB - Done",
        ]
        progress = tqdm(total=len(stages), desc="Cohort creation", unit="stage", dynamic_ncols=True, leave=True)

        progress.set_postfix_str(stages[0])
        if query_or_yaml_file.endswith(".yaml") or query_or_yaml_file.endswith(".yml"):
            try:
                cohort_config = load_cohort_creation_config(query_or_yaml_file)
                tqdm.write(f"configuration specified in {query_or_yaml_file} loaded successfully")
            except FileNotFoundError:
                notify_users(
                    "specified cohort creation configuration file does not exist. Make sure "
                    "the configuration file name with path is specified correctly."
                )
                return None
            except ValidationError as ex:
                notify_users(f"cohort creation configuration yaml file is not valid with validation error: {ex}")
                return None

            query = self._query_builder.build_query_cohort_creation(cohort_config)
        else:
            query = clean_string(query_or_yaml_file)
        progress.update(1)

        progress.set_postfix_str(stages[1])
        omop_session = self.omop_db.get_session()
        try:
            # Execute read-only query from OMOP CDM database
            result = self.omop_db.execute_query(query)
            if result:
                # Create CohortDefinition
                cohort_def = CohortDefinition(
                    name=cohort_name,
                    description=description,
                    created_date=datetime.now().date(),
                    creation_info=clean_string(query),
                    created_by=created_by,
                )
                cohort_def_id = self.bias_db.create_cohort_definition(cohort_def, progress_obj=tqdm)
                progress.update(1)

                progress.set_postfix_str(stages[2])
                # Store cohort_definition and cohort data into BiasDatabase
                cohort_df = pd.DataFrame(result)
                cohort_df["cohort_definition_id"] = cohort_def_id
                cohort_df = cohort_df.rename(columns={"person_id": "subject_id"})
                self.bias_db.create_cohort_in_bulk(cohort_df)
                progress.update(1)

                tqdm.write(f"Cohort {cohort_name} successfully created.")
                return CohortData(cohort_id=cohort_def_id, bias_db=self.bias_db, omop_db=self.omop_db)
            else:
                progress.update(2)
                notify_users("No cohort is created due to empty results being returned from query")
                return None
        except Exception as e:
            progress.update(2)
            notify_users(f"Error executing query: {e}")
            if omop_session is not None:
                omop_session.close()
            return None

    def get_cohorts_concept_stats(
        self, cohorts: List[int], concept_type: str = "condition_occurrence", filter_count: int = 0, vocab=None
    ):
        cohort_concept_stats = [
            self.bias_db.get_cohort_concept_stats(
                c, self._query_builder, concept_type=concept_type, filter_count=filter_count, vocab=vocab
            )
            for c in cohorts
        ]
        hierarchies = [
            ConceptHierarchy.build_concept_hierarchy_from_results(
                c, concept_type, c_stats.get(concept_type, []), filter_count=filter_count, vocab=vocab
            )
            for c, c_stats in zip(cohorts, cohort_concept_stats)
        ]
        return reduce(lambda h1, h2: h1.union(h2), hierarchies).to_dict()

    def compare_cohorts(self, cohort_id_1: int, cohort_id_2: int):
        """
        Compare the distributions of two cohorts in BiasDatabase.
        """
        results = []
        for variable in self.bias_db.cohort_distribution_variables:
            cohort_1_stats = self.bias_db.get_cohort_distributions(cohort_id_1, variable=variable)
            cohort_2_stats = self.bias_db.get_cohort_distributions(cohort_id_2, variable=variable)
            cohort_1_probs = [entry["probability"] for entry in cohort_1_stats]
            cohort_2_probs = [entry["probability"] for entry in cohort_2_stats]
            dist = hellinger_distance(cohort_1_probs, cohort_2_probs)
            results.append({f"{variable}_hellinger_distance": dist})

        return results

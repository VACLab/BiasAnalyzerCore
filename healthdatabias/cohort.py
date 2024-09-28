from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from typing import List, Dict
from models import CohortDefinition, Cohort
from database import OMOPCDMDatabase, BiasDatabase


class CohortAction:
    def __init__(self, omop_db: OMOPCDMDatabase, bias_db: BiasDatabase):
        self.omop_db = omop_db
        self.bias_db = bias_db

    def create_cohort(self, cohort_name: str, description: str, query: str, created_by: str):
        """
        Create a new cohort by executing a query on OMOP CDM database
        and storing the result in BiasDatabase.
        """
        omop_session = self.omop_db.get_session()
        try:
            # Execute read-only query from OMOP CDM database
            result = omop_session.execute(query).fetchall()

            # Create CohortDefinition
            cohort_def = CohortDefinition(
                name=cohort_name,
                description=description,
                created_date=datetime.now().date(),
                creation_info=query,
                created_by=created_by
            )
            cohort_def_id = self.bias_db.create_cohort_definition(cohort_def)

            # Store cohort_definition and cohort data into BiasDatabase
            for row in result:
                cohort = Cohort(
                    subject_id=row['person_id'],  # Assuming person_id column in the result
                    cohort_definition_id=cohort_def_id,
                    cohort_start_date=row['cohort_start_date'],
                    cohort_end_date=row['cohort_end_date']
                )
                self.bias_db.create_cohort(cohort)
            print(f"Cohort {cohort_name} successfully created.")
        except SQLAlchemyError as e:
            print(f"Error executing query: {e}")
        finally:
            omop_session.close()

    def get_cohort_definitions(self):
        """
        Fetch all cohort definitions from BiasDatabase.
        """
        return self.bias_db.get_cohort_definitions()

    def get_cohort(self, cohort_id: int):
        """
        Fetch cohort data for a specific cohort in BiasDatabase.
        """
        return self.bias_db.get_cohort(cohort_id)

    def get_cohort_stats(self, cohort_id: int):
        """
        Get aggregation statistics for a specific cohort in BiasDatabase.
        """
        return self.bias_db.get_cohort_stats(cohort_id)

    def compare_cohorts(self, cohort_id_1: int, cohort_id_2: int):
        """
        Compare the distributions of two cohorts in BiasDatabase.
        """
        cohort_1_stats = self.bias_db.get_cohort_stats(cohort_id_1)
        cohort_2_stats = self.bias_db.get_cohort_stats(cohort_id_2)

        # Compare the statistics, could be comparing distributions, averages, etc.
        comparison_result = {
            cohort_id_1: cohort_1_stats,
            cohort_id_2: cohort_2_stats
        }

        return comparison_result

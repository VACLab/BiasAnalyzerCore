import duckdb
from models import Cohort, CohortDefinition
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection


class BiasDatabase:
    def __init__(self):
        self.conn = duckdb.connect(database=':memory:')
        self.cursor = self.conn.cursor()

    def create_cohort_definition(self):
        self.cursor.execute('''
                    CREATE TABLE cohort_definition (
                        id integer PRIMARY KEY,
                        name VARCHAR,
                        description VARCHAR,
                        created_date DATE,
                        creation_info VARCHAR,
                        created_by VARCHAR
                    )
                ''')
        print("Cohort Definition table created.")

    def create_cohort(self):
        self.cursor.execute('''
            CREATE TABLE cohort (
                subject_id BIGINT,
                cohort_definition_id INTEGER,
                cohort_start_date DATE,
                cohort_end_date DATE,
                PRIMARY KEY (cohort_definition_id, subject_id),
                FOREIGN KEY (cohort_definition_id) REFERENCES cohort_definition(id)
            )
        ''')
        print("Cohort table created.")

    def insert_cohort_definition(self, cohort_definition: CohortDefinition):
        self.cursor.execute('''
            INSERT INTO cohort_definition (id, name, description, created_date, creation_info, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            cohort_definition.id,
            cohort_definition.name,
            cohort_definition.description,
            cohort_definition.created_date or datetime.now(),
            cohort_definition.creation_info,
            cohort_definition.created_by
        ))
        print("Inserted cohort definition.")

    # Method to insert cohort data
    def insert_cohort(self, cohort: Cohort):
        self.cursor.execute('''
            INSERT INTO cohort (subject_id, cohort_definition_id, cohort_start_date, cohort_end_date)
            VALUES (?, ?, ?, ?)
        ''', (
            cohort.subject_id,
            cohort.cohort_definition_id,
            cohort.cohort_start_date,
            cohort.cohort_end_date
        ))
        print("Inserted cohort.")

    def close(self):
        self.cursor.close()
        self.conn.close()
        print("Connection closed.")

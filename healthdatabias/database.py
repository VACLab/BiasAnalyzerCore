import duckdb
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
from healthdatabias.models import Cohort, CohortDefinition


class BiasDatabase:
    _instance = None  # indicating a singleton with only one instance of the class ever created
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(BiasDatabase, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialize()  # Initialize only once
        return cls._instance

    def _initialize(self):
        # by default, duckdb uses in memory database
        self.conn = duckdb.connect(':memory:')
        self._create_cohort_definition_table()
        self._create_cohort_table()

    def _create_cohort_definition_table(self):
        self.conn.execute('CREATE SEQUENCE id_sequence START 1')
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cohort_definition (
                      id INTEGER DEFAULT nextval('id_sequence'), 
                      name VARCHAR NOT NULL, 
                      description VARCHAR, 
                      created_date DATE, 
                      creation_info VARCHAR, 
                      created_by VARCHAR
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
                PRIMARY KEY (cohort_definition_id, subject_id),
                FOREIGN KEY (cohort_definition_id) REFERENCES cohort_definition(id)
            )
        ''')
        print("Cohort table created.")

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
        print("Inserted cohort definition.")
        self.conn.execute("SELECT id from cohort_definition ORDER BY id DESC LIMIT 1")
        return self.conn.fetchone()

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
        print("Inserted cohort.")

    def get_cohort_definitions(self):
        results = self.conn.execute('''
        SELECT id, name, description, created_date, creation_info, created_by FROM cohort_definition
        ''')
        return results.fetchall()

    def get_cohort(self, cohort_definition_id):
        results = self.conn.execute(f'''
        SELECT subject_id, cohort_definition_id, cohort_start_date, cohort_end_date FROM cohort 
        WHERE cohort_definition_id = {cohort_definition_id}
        ''')
        return results.fetchall()

    def get_cohort_stats(self, cohort_definition_id: int):
        """
        Get aggregation statistics for a cohort from the cohort table.
        """
        try:
            # Query the cohort data to get basic statistics
            stats_query = f'''
                SELECT
                    COUNT(subject_id) AS subject_count,
                    MIN(cohort_start_date) AS earliest_start_date,
                    MAX(cohort_start_date) AS latest_start_date,
                    MIN(cohort_end_date) AS earliest_end_date,
                    MAX(cohort_end_date) AS latest_end_date
                FROM cohort
                WHERE cohort_definition_id = {cohort_definition_id}
            '''
            result = self.conn.execute(stats_query).fetchall()

            # Convert result into a dictionary for easy access
            stats = {
                "subject_count": result[0][0],
                "earliest_start_date": result[0][1],
                "latest_start_date": result[0][2],
                "earliest_end_date": result[0][3],
                "latest_end_date": result[0][4]
            }
            return stats

        except Exception as e:
            print(f"Error getting cohort statistics: {e}")
            return None

    def close(self):
        self.conn.close()
        BiasDatabase._instance = None
        print("Connection to BiasDatabase closed.")


class OMOPCDMDatabase:
    _instance = None  # indicating a singleton with only one instance of the class ever created
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(OMOPCDMDatabase, cls).__new__(cls)
            cls._instance._initialize(*args, **kwargs)  # Initialize only once
        return cls._instance

    def _initialize(self, db_url):
        try:
            self.engine = create_engine(
                db_url,
                echo=False,
                connect_args={'options': '-c default_transaction_read_only=on'}  # Enforce read-only transactions
            )
            self.Session = sessionmaker(bind=self.engine)
            print("Connected to the OMOP CDM database (read-only).")
        except SQLAlchemyError as e:
            print(f"Failed to connect to the database: {e}")

    def get_session(self):
        # Provide a new session for read-only queries
        return self.Session()

    def close(self):
        # Dispose of the connection (if needed)
        self.engine.dispose()
        OMOPCDMDatabase._instance = None
        print("Connection to the OMOP CDM database closed.")

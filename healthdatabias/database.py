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
        self.omop_cdm_db_url = None
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
                PRIMARY KEY (cohort_definition_id, subject_id),
                FOREIGN KEY (cohort_definition_id) REFERENCES cohort_definition(id)
            )
        ''')
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

    def get_cohort_definitions(self):
        results = self.conn.execute('''
        SELECT id, name, description, created_date, creation_info, created_by FROM cohort_definition
        ''')
        headers = [desc[0] for desc in results.description]
        rows = results.fetchall()
        return [dict(zip(headers, row)) for row in rows]

    def get_cohort(self, cohort_definition_id, count):
        results = self.conn.execute(f'''
        SELECT subject_id, cohort_definition_id, cohort_start_date, cohort_end_date FROM cohort 
        WHERE cohort_definition_id = {cohort_definition_id} limit {count}
        ''')
        headers = [desc[0] for desc in results.description]
        rows = results.fetchall()
        return [dict(zip(headers, row)) for row in rows]

    def get_cohort_basic_stats(self, cohort_definition_id: int):
        """
        Get aggregation statistics for a cohort from the cohort table.
        """
        try:
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
                    AVG(duration_days) AS avg_duration_days,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_days) AS median_duration,
                    STDDEV(duration_days) AS stddev_duration
                FROM cohort_Duration
                
            '''
            result = self.conn.execute(stats_query).fetchall()

            # Convert result into a dictionary for easy access
            stats = {
                "subject_count": result[0][0],
                "earliest_start_date": result[0][1],
                "latest_start_date": result[0][2],
                "earliest_end_date": result[0][3],
                "latest_end_date": result[0][4],
                "min_duration_days": result[0][5],
                "max_duration_days": result[0][6],
                "avg_duration_days": round(result[0][7], 2) if result[0][7] is not None else None,
                "median_duration_days": int(result[0][8]) if result[0][8] is not None else None,
                "stddev_duration_days": round(result[0][9], 2) if result[0][9] is not None else None
            }
            return stats

        except Exception as e:
            print(f"Error computing cohort basic statistics: {e}")
            return None

    def get_cohort_age_distributions(self, cohort_definition_id: int):
        """
        Get age distribution statistics for a cohort from the cohort table.
        """
        try:
            if self.omop_cdm_db_url is not None:
                # need to create person table from OMOP CDM postgreSQL database
                self.conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS person AS 
                    SELECT * from postgres_scan('{self.omop_cdm_db_url}', 'public', 'person')
                """)
            query = f'''
                WITH Age_Cohort AS (
                    SELECT p.person_id, EXTRACT(YEAR FROM c.cohort_start_date) - p.year_of_birth AS age 
                    FROM cohort c JOIN person p ON c.subject_id = p.person_id
                    WHERE c.cohort_definition_id = {cohort_definition_id}
                    )
                -- Calculate age distribution statistics    
                SELECT
                    COUNT(*) AS total_count,
                    MIN(age) AS min_age,
                    MAX(age) AS max_age,
                    AVG(age) AS avg_age,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) AS median_age,
                    STDDEV(age) as stddev_age
                FROM Age_Cohort                
            '''
            result = self.conn.execute(query).fetchall()

            # Convert result into a dictionary for easy access
            stats = {
                "total_count": result[0][0],
                "min_age": result[0][1],
                "max_age": result[0][2],
                "average_age": round(result[0][3], 2) if result[0][3] is not None else None,
                "median_age": int(result[0][4]) if result[0][4] is not None else None,
                "stddev_age": round(result[0][5], 2) if result[0][5] is not None else None
            }
            return stats

        except Exception as e:
            print(f"Error computing cohort age distributions: {e}")
            return None

    def get_cohort_gender_distributions(self, cohort_definition_id: int):
        """
        Get gender distribution statistics for a cohort from the cohort table.
        """
        try:
            if self.omop_cdm_db_url is not None:
                # need to create person table from OMOP CDM postgreSQL database
                self.conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS person AS 
                    SELECT * from postgres_scan('{self.omop_cdm_db_url}', 'public', 'person')
                """)

            query = f'''
                SELECT
                    p.gender_concept_id, 
                    COUNT(*) AS gender_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM cohort c JOIN person p ON c.subject_id = p.person_id 
                WHERE c.cohort_definition_id = {cohort_definition_id}
                GROUP BY p.gender_concept_id
            '''
            result = self.conn.execute(query).fetchall()

            # Convert result into a dictionary for easy access
            stats = {
                "gender": 'male' if result[0][0] == 8507 else 'female' if result[0][0] == 8532 else 'other',
                "count": result[0][1],
                "percentage": round(result[0][2], 2) if result[0][2] is not None else None
            }
            return stats

        except Exception as e:
            print(f"Error computing cohort gender distributions: {e}")
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

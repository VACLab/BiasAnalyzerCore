from pydantic import BaseModel, StrictStr, ConfigDict, field_validator, model_validator
from typing import Optional, Literal, List, Union
from datetime import date


DOMAIN_MAPPING = {
    "condition_occurrence": {
        "table": "condition_occurrence",
        "concept_id": "condition_concept_id",
        "start_date": "condition_start_date",
        "end_date": "condition_end_date",
        "default_vocab": "SNOMED"  # for use by concept prevalence query
    },
    "drug_exposure": {
        "table": "drug_exposure",
        "concept_id": "drug_concept_id",
        "start_date": "drug_exposure_start_date",
        "end_date": "drug_exposure_end_date",
        "default_vocab": "RxNorm"  # for use by concept prevalence query
    },
    "procedure_occurrence": {
        "table": "procedure_occurrence",
        "concept_id": "procedure_concept_id",
        "start_date": "procedure_date",
        "end_date": "procedure_date",
        "default_vocab": "SNOMED"  # for use by concept prevalence query
    },
    "visit_occurrence": {
        "table": "visit_occurrence",
        "concept_id": "visit_concept_id",
        "start_date": "visit_start_date",
        "end_date": "visit_end_date",
        "default_vocab": "SNOMED"  # for use by concept prevalence query
    },
    "measurement": {
        "table": "measurement",
        "concept_id": "measurement_concept_id",
        "start_date": "measurement_date",
        "end_date": "measurement_date",
        "default_vocab": "LOINC"  # for use by concept prevalence query
    },
    "observation": {
        "table": "observation",
        "concept_id": "observation_concept_id",
        "start_date": "observation_date",
        "end_date": "observation_date",
        "default_vocab": "SNOMED"  # for use by concept prevalence query
    },
    "date": {  # Special case for static timestamps
        "table": None,
        "concept_id": None,
        "start_date": "timestamp",
       "end_date": "timestamp",
        "default_vocab": None
    }
}

EVENT_TYPE_LITERAL = Literal[tuple(DOMAIN_MAPPING.keys())]


###===========System Configuration==============###
class RootOMOPCDM(BaseModel):
    model_config = ConfigDict(extra='ignore')
    username: StrictStr
    password: StrictStr
    hostname: StrictStr
    database: StrictStr
    port: int


class Configuration(BaseModel):
    model_config = ConfigDict(extra='ignore')
    root_omop_cdm_database: RootOMOPCDM
###===========System Configuration==============###

###===========CohortDefinition Model==============###
class CohortDefinition(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    description: str
    created_date: date
    creation_info: str
    created_by: str
###===========CohortDefinition Model==============###

###===========Cohort Model====================###
class Cohort(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cohort_definition_id: int
    subject_id: int
    cohort_start_date: Optional[date]
    cohort_end_date: Optional[date]
###===========Cohort Model====================###

###=========CohortCreationConfig==================###
class DemographicsCriteria(BaseModel):
    # Gender with "male" and "female" as valid input
    gender: Optional[Literal['male', 'female']] = None
    # Minimum birth year
    min_birth_year: Optional[int] = None
    max_birth_year: Optional[int] = None

    @field_validator("max_birth_year")
    def validate_birth_years(cls, max_birth_year, info):
        min_birth_year = info.data.get("min_birth_year")
        if min_birth_year is not None and max_birth_year is not None:
            if max_birth_year < min_birth_year:
                raise ValueError("max_birth_year must be greater than or equal to min_birth_year")
        return max_birth_year


class TemporalEvent(BaseModel):
    # Generate Literal type from DOMAIN_MAPPING keys
    event_type: EVENT_TYPE_LITERAL
    event_concept_id: Optional[int] = None  # Optional for 'date' event_type
    event_instance: Optional[int] = None  # Optional: Specific occurrence (e.g., 2nd hospitalization)
    timestamp: Optional[str] = None

    @model_validator(mode="before")
    def validate_event_type(cls, values):
        event_type = values.get("event_type")
        if event_type == "date" and not values.get("timestamp"):
            raise ValueError("'date' event_type must have a 'timestamp'")
        if event_type != "date" and not values.get("event_concept_id"):
            raise ValueError(f"'{event_type}' requires an 'event_concept_id'")
        return values


class TemporalEventGroup(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    operator: Literal["AND", "OR", "NOT", "BEFORE"]
    events: List[Union[TemporalEvent, "TemporalEventGroup"]]  # A list of events or nested operators
    interval: Optional[List[Union[int, None]]] = None  # [start, end] interval only applying for BEFORE operator

    @model_validator(mode="before")
    def validate_interval_logic(cls, values):
        """
        Validate interval structure and logic for all operators, though only used for BEFORE.
        Ensures interval is None or a list of two elements [start, end], with start <= end if both are integers.
        For AND, OR, NOT, interval is validated but ignored in SQL generation.
        """
        interval = values.get("interval")
        """Ensure interval is logically consistent which is only used for operator 'BEFORE'."""
        if interval is not None:
            if not isinstance(interval, list) or len(interval) != 2:
                raise ValueError("Interval must be a list with exactly two elements: [start, end].")
            start, end = interval
            if start is not None and end is not None and start > end:
                raise ValueError("Interval start cannot be greater than interval end.")
        return values

    @model_validator(mode="before")
    def validate_events_list(cls, values):
        operator = values.get("operator")
        events = values.get("events")

        if not events or len(events) == 0:
            raise ValueError(f"'{operator}' operator requires a non-empty 'events' list")

        if operator == "NOT" and len(events) != 1:
            raise ValueError("'NOT' operator must have exactly one event in 'events'")

        if operator == "BEFORE" and len(events) != 2:
            raise ValueError("'BEFORE' operator must have exactly two events in 'events'")

        return values

    def get_interval_sql(self, e1_alias='e1', e2_alias='e2') -> str:
        """Generate SQL for the interval."""
        if not self.interval:  # pragma: no cover
            return ""
        start = self.interval[0] if self.interval[0] is not None else 0
        end = self.interval[1] if self.interval[1] is not None else 99999
        return f"AND {e2_alias}.event_start_date - {e1_alias}.event_start_date BETWEEN {start} AND {end}"


class CohortCreationCriteria(BaseModel):
    demographics: Optional[DemographicsCriteria] = None  # Optional
    temporal_events: Optional[List[TemporalEventGroup]] = None  # List of temporal event operators


class CohortCreationConfig(BaseModel):
    # SQL query template name
    # cohort creation criteria
    inclusion_criteria: CohortCreationCriteria
    exclusion_criteria: Optional[CohortCreationCriteria] = None
###=========CohortCreationConfig==================###

from pydantic import BaseModel, StrictStr, ConfigDict, field_validator, model_validator
from typing import Optional, Literal, List, Union
from datetime import date


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
class ConditionCriteria(BaseModel):
    # SNOMED Condition concept ID in OMOP (e.g., 37311061 for COVID-19)
    condition_concept_id: int


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
    event_type: Literal['condition_occurrence', 'visit_occurrence', 'date']
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

    @field_validator("interval", mode="before")
    def validate_interval_structure(cls, value):
        """Ensure interval is a list with exactly two elements, or None."""
        if value is None:
            return value
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError("Interval must be a list with exactly two elements: [start, end].")
        return value

    @model_validator(mode="before")
    def validate_interval_logic(cls, values):
        operator = values.get("operator")
        interval = values.get("interval")
        """Ensure interval is logically consistent when operator is 'BEFORE'."""
        if operator == "BEFORE" and interval is not None:
            start, end = interval
            if start is not None and not isinstance(start, int):
                raise ValueError("Interval start must be an integer or None.")
            if end is not None and not isinstance(end, int):
                raise ValueError("Interval end must be an integer or None.")
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
        if not self.interval:
            return ""
        start = self.interval[0] if self.interval[0] is not None else 0
        end = self.interval[1] if self.interval[1] is not None else 99999
        return f"AND {e2_alias}.event_start_date - {e1_alias}.event_start_date BETWEEN {start} AND {end}"


class ConditionCohortCriteria(BaseModel):
    demographics: Optional[DemographicsCriteria] = None  # Optional
    temporal_events: Optional[List[TemporalEventGroup]] = None  # List of temporal event operators


class CohortCreationConfig(BaseModel):
    # SQL query template name
    # cohort creation criteria
    inclusion_criteria: ConditionCohortCriteria
    exclusion_criteria: Optional[ConditionCohortCriteria] = None
###=========CohortCreationConfig==================###

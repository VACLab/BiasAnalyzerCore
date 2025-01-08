from pydantic import BaseModel, StrictStr, ConfigDict, field_validator
from typing import Optional, Literal
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
    cohort_start_date: date
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

class ConditionCohortCriteria(BaseModel):
    condition_occurrence: ConditionCriteria
    demographics: Optional[DemographicsCriteria] = None  # Optional

class CohortCreationConfig(BaseModel):
    # SQL query template name
    template_name: str
    # cohort creation criteria
    inclusion_criteria: ConditionCohortCriteria
    exclusion_criteria: Optional[ConditionCohortCriteria] = None

class TemporalEventCriteria(BaseModel):
    event_concept_id: int  # Required: Event concept ID
    event_instance: Optional[int] = None  # Optional: Specific occurrence (e.g., 2nd hospitalization)
    operator: Optional[Literal["AND", "OR", "NOT"]] = "AND"  # Default to AND if not specified
###=========CohortCreationConfig==================###
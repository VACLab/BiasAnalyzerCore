from pydantic import BaseModel, StrictStr, ConfigDict, Field
from typing import Optional
from datetime import date


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


class CohortDefinition(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    description: str
    created_date: date
    creation_info: str
    created_by: str


class Cohort(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    cohort_definition_id: int
    subject_id: int
    cohort_start_date: date
    cohort_end_date: Optional[date]


class CohortCriteria(BaseModel):
    # SNOMED Condition concept ID in OMOP (e.g., 37311061 for COVID-19)
    condition_concept_id: int
    # Gender concept ID (e.g., 8532 for female)
    gender_concept_id: Optional[int]
    # Minimum birth year
    min_birth_year: Optional[int]


class CohortCreationConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    # SQL query template name
    template_name: str
    # cohort creation criteria
    criteria: CohortCriteria

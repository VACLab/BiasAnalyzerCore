from pydantic import BaseModel, StrictStr, ConfigDict
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
    cohort_end_date: date

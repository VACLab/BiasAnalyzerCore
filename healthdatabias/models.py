from pydantic import BaseModel, StrictStr
from datetime import date


class RootOMOPCDM(BaseModel):
    username: StrictStr
    password: StrictStr
    hostname: StrictStr
    database: StrictStr
    port: int


class Configuration(BaseModel):
    root_omop_cdm_database: RootOMOPCDM


class CohortDefinition(BaseModel):
    name: str
    description: str
    created_date: date
    creation_info: str
    created_by: str

    class Config:
        orm_mode = True  # Enable compatibility with ORMs like SQLAlchemy


class Cohort(BaseModel):
    cohort_definition_id: int
    subject_id: int
    cohort_start_date: date
    cohort_end_date: date

    class Config:
        orm_mode = True  # Enable compatibility with ORMs like SQLAlchemy




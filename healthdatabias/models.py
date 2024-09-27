from pydantic import BaseModel
from datetime import date
from healthdatabias.config import load_config


class HealthDataBias:
    def __init__(self):
        self.config = {}

    def set_config(self, config_file_path):
        if not self.config:
            self.config = load_config(config_file_path)


class CohortDefinition(BaseModel):
    id: int
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




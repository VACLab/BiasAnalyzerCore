import yaml
from biasanalyzer.models import Configuration, CohortCreationConfig


def load_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        Configuration(**config)
        return config


def load_cohort_creation_config(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        CohortCreationConfig(**config)
        return config

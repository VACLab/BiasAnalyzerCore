import os
import pytest
from biasanalyzer import __version__
import logging


def test_version():
    assert __version__ == '0.1.0'

@pytest.mark.usefixtures
def test_set_config(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        fresh_bias_obj.set_config('non_existent_config_file.yaml')
    assert 'does not exist' in caplog.text

    caplog.clear()
    with caplog.at_level(logging.ERROR):
        invalid_config_file = os.path.join(os.path.dirname(__file__), 'assets', 'config',
                                           'test_invalid_config.yaml')
        fresh_bias_obj.set_config(invalid_config_file)
    assert 'is not valid' in caplog.text


@pytest.mark.usefixtures
def test_set_root_omop(caplog, fresh_bias_obj):
    caplog.clear()
    with caplog.at_level(logging.INFO):
        fresh_bias_obj.set_root_omop()
    assert 'no valid configuration' in caplog.text

    caplog.clear()
    with caplog.at_level(logging.INFO):
        config_file_with_unsupported_db_type = os.path.join(os.path.dirname(__file__), 'assets', 'config',
                                                            'test_config_unsupported_db_type.yaml')
        fresh_bias_obj.set_config(config_file_with_unsupported_db_type)
        fresh_bias_obj.set_root_omop()
    assert 'Unsupported database type' in caplog.text

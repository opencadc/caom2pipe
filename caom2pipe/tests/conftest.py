import os
import pytest

from caom2pipe import manage_composable as mc
import test_conf as tc


@pytest.fixture(scope='function')
def test_config():
    test_config = mc.Config()
    test_config.working_directory = tc.THIS_DIR
    test_config.collection = 'OMM'
    test_config.netrc_file = os.path.join(tc.TEST_DATA_DIR, 'test_netrc')
    test_config.work_file = 'todo.txt'
    test_config.logging_level = 'DEBUG'
    test_config.log_file_directory = tc.TEST_DATA_DIR
    test_config.failure_fqn = f'{tc.TEST_DATA_DIR}/fail.txt'
    test_config.failure_log_file_name = 'fail.txt'
    test_config.retry_fqn = f'{tc.TEST_DATA_DIR}/retry.txt'
    test_config.retry_file_name = 'retry.txt'
    test_config.success_fqn = f'{tc.TEST_DATA_DIR}/good.txt'
    test_config.success_log_file_name = 'good.txt'
    test_config.rejected_fqn = f'{tc.TEST_DATA_DIR}/rejected.yml'
    test_config.progress_fqn = f'{tc.TEST_DATA_DIR}/progress.txt'
    test_config.resource_id = 'ivo://cadc.nrc.ca/sc2repo'
    test_config.features.run_in_airflow = False
    test_config.features.use_file_names = False
    test_config._report_fqn = (
        f'{test_config.log_file_directory}/' f'test_report.txt'
    )
    test_config.storage_inventory_resource_id = 'ivo://cadc.nrc.ca/TEST'
    test_config.stream = 'TEST'
    for f_name in [
        test_config.failure_fqn,
        test_config.success_fqn,
        test_config.retry_fqn,
    ]:
        if os.path.exists(f_name):
            os.unlink(f_name)
    return test_config

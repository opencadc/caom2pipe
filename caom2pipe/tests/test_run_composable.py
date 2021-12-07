# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import distutils
import logging
import pytest
import glob
import os

from astropy.table import Table
from datetime import datetime, timedelta, timezone

from unittest.mock import Mock, patch, ANY
import test_conf as tc

from caom2 import SimpleObservation, Algorithm
from caom2pipe import data_source_composable as dsc
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import run_composable as rc
from caom2pipe import name_builder_composable as b


STATE_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_state.yml')
TEST_BOOKMARK = 'test_bookmark'
TEST_COMMAND = 'test_command'
TEST_DIR = f'{tc.TEST_DATA_DIR}/run_composable'
TEST_SOURCE = (
    f'{distutils.sysconfig.get_python_lib()}/test_command/' f'test_command.py'
)


@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.manage_composable.read_obs_from_file')
@patch('caom2pipe.manage_composable.write_obs_to_file')
def test_run_todo_list_dir_data_source(
    write_obs_mock,
    read_obs_mock,
    visit_meta_mock,
    test_config,
):
    read_obs_mock.side_effect = _mock_read
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.data_sources = [tc.TEST_FILES_DIR]
    test_config.data_source_extensions = ['.fits']

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(
        config=test_config, chooser=test_chooser
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert visit_meta_mock.called, 'expect visit call'
    visit_meta_mock.assert_called_with()
    assert write_obs_mock.called, 'expect write call'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.manage_composable.read_obs_from_file')
@patch('caom2pipe.manage_composable.write_obs_to_file')
def test_run_todo_list_dir_data_source_v(
    write_obs_mock,
    read_obs_mock,
    visit_meta_mock,
    clients_mock,
    test_config,
):
    read_obs_mock.side_effect = _mock_read
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.data_sources = [tc.TEST_FILES_DIR]
    test_config.data_source_extensions = ['.fits']
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.features.supports_latest_client = True
    test_result = rc.run_by_todo(config=test_config)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert visit_meta_mock.called, 'expect visit call'
    visit_meta_mock.assert_called_with()
    assert read_obs_mock.called, 'read_obs not called'
    assert write_obs_mock.called, 'write_obs mock not called'
    assert not (
        clients_mock.return_value.metadata_client.read.called
    ), 'scrape, should be no client access'
    assert not (
        clients_mock.return_value.data_client.get_file.called
    ), 'scrape, should be no client access'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_todo_list_dir_data_source_invalid_fname_v(
    clients_mock, test_config
):
    test_dir = os.path.join('/test_files', '1')
    test_fqn = os.path.join(test_dir, 'abc.fits.gz')

    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.data_sources = [test_dir]
    test_config.data_source_extensions = ['.fits', '.fits.gz']
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.log_to_file = False
    test_config.features.supports_latest_client = True

    if os.path.exists(test_config.failure_fqn):
        os.unlink(test_config.failure_fqn)
    if os.path.exists(test_config.retry_fqn):
        os.unlink(test_config.retry_fqn)

    if not os.path.exists(test_dir):
        os.mkdir(test_dir)
    if not os.path.exists(test_fqn):
        with open(test_fqn, 'w') as f:
            f.write('abc')

    class TestStorageName(mc.StorageName):
        def __init__(self, entry):
            self._obs_id = os.path.basename(entry)
            self._source_names = [entry]

        def is_valid(self):
            return False

    class TestStorageNameInstanceBuilder(b.StorageNameInstanceBuilder):
        def __init__(self):
            pass

        def build(self, entry):
            return TestStorageName(entry)

    try:
        test_builder = TestStorageNameInstanceBuilder()
        test_chooser = ec.OrganizeChooser()
        test_result = rc.run_by_todo(
            config=test_config,
            chooser=test_chooser,
            name_builder=test_builder,
        )
        assert test_result is not None, 'expect a result'
        assert test_result == -1, 'expect failure, because of file naming'
        assert os.path.exists(test_config.failure_fqn), 'expect failure file'
        assert os.path.exists(test_config.retry_fqn), 'expect retry file'
        assert (
            not clients_mock.metadata_client.read.called
        ), 'repo client read access happens after is_valid call'
        assert not (
            clients_mock.data_client.get_file.called
        ), 'bad file naming, should be no client access'
    finally:
        if os.path.exists(test_fqn):
            os.unlink(test_fqn)
        if os.path.exists(test_dir):
            logging.error(os.listdir(test_dir))
            os.rmdir(test_dir)


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_todo_file_data_source(clients_mock, test_config):
    clients_mock.return_value.data_client.get_file_info.return_value = None
    clients_mock.return_value.metadata_client.read.return_value = (
        SimpleObservation(
            collection=test_config.collection,
            observation_id='def',
            algorithm=Algorithm('test'),
        )
    )

    if os.path.exists(test_config.success_fqn):
        os.unlink(test_config.success_fqn)

    test_config.work_fqn = f'{TEST_DIR}/todo.txt'
    test_config.task_types = [mc.TaskType.VISIT]
    test_config.log_to_file = True

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(
        config=test_config, chooser=test_chooser
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert os.path.exists(test_config.success_fqn), 'expect success file'

    with open(test_config.success_fqn) as f:
        content = f.read()
        # the obs id and file name
        assert 'def def.fits' in content, 'wrong success message'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_todo_file_data_source_v(clients_mock, test_config):
    test_config.features.supports_latest_client = True
    test_cert_file = os.path.join(TEST_DIR, 'test_proxy.pem')
    test_config.proxy_fqn = test_cert_file
    clients_mock.return_value.metadata_client.read.side_effect = Mock(
        return_value=SimpleObservation(
            collection=test_config.collection,
            observation_id='def',
            algorithm=Algorithm('test'),
        )
    )

    if os.path.exists(test_config.success_fqn):
        os.unlink(test_config.success_fqn)

    test_config.work_fqn = f'{TEST_DIR}/todo.txt'
    test_config.task_types = [mc.TaskType.VISIT]
    test_config.log_to_file = True

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(
        config=test_config, chooser=test_chooser
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert os.path.exists(test_config.success_fqn), 'expect success file'

    with open(test_config.success_fqn) as f:
        content = f.read()
        # the obs id and file name
        assert 'def def.fits' in content, 'wrong success message'
    assert (
        clients_mock.return_value.metadata_client.read.called
    ), 'expect e call'
    clients_mock.return_value.metadata_client.read.assert_called_with(
        'OMM', 'def'
    ), 'wrong e args'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.data_source_composable.CadcTapClient')
@patch('caom2pipe.client_composable.query_tap_client')
@patch(
    'caom2pipe.execute_composable.MetaVisit._visit_meta'
)
def test_run_state(
    visit_meta_mock,
    tap_query_mock,
    tap_mock,
    clients_mock,
    test_config,
):
    # tap_mock is used by the data_source_composable class
    visit_meta_mock.side_effect = _mock_visit
    clients_mock.return_value.metadata_client.read.side_effect = _mock_read2
    tap_query_mock.side_effect = _mock_get_work

    test_end_time = datetime.fromtimestamp(1579740838, tz=timezone.utc)
    start_time = test_end_time - timedelta(seconds=900)
    _write_state(start_time)

    test_config.task_types = [mc.TaskType.INGEST]
    test_config.state_fqn = STATE_FILE
    test_config.interval = 10
    individual_log_file = (
        f'{test_config.log_file_directory}/NEOS_SCI_2015347000000_clean.log'
    )
    if os.path.exists(test_config.progress_fqn):
        os.unlink(test_config.progress_fqn)
    if os.path.exists(test_config.success_fqn):
        os.unlink(test_config.success_fqn)
    if os.path.exists(individual_log_file):
        os.unlink(individual_log_file)

    test_chooser = ec.OrganizeChooser()
    # use_local_files set so run_by_state chooses QueryTimeBoxDataSourceTS
    test_config.use_local_files = False
    test_result = rc.run_by_state(
        config=test_config,
        chooser=test_chooser,
        bookmark_name=TEST_BOOKMARK,
        end_time=test_end_time,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert visit_meta_mock.called, 'expect visit meta call'
    visit_meta_mock.assert_called_once_with()

    test_state = mc.State(STATE_FILE)
    test_bookmark = test_state.get_bookmark(TEST_BOOKMARK)
    assert test_bookmark == test_end_time, 'wrong time'
    assert os.path.exists(test_config.progress_fqn), 'expect progress file'
    assert os.path.exists(
        test_config.success_fqn
    ), 'log_to_file set to false, no success file'
    assert not os.path.exists(
        individual_log_file
    ), f'log_to_file is False, no entry log'

    # test that runner does nothing when times haven't changed
    start_time = test_end_time
    _write_state(start_time)
    visit_meta_mock.reset_mock()
    test_result = rc.run_by_state(
        config=test_config,
        chooser=test_chooser,
        bookmark_name=TEST_BOOKMARK,
        end_time=test_end_time,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert not visit_meta_mock.called, 'expect no visit_meta call'


@patch('caom2pipe.data_source_composable.CadcTapClient')
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.client_composable.query_tap_client')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
def test_run_state_log_to_file_true(
    visit_meta_mock,
    tap_mock,
    clients_mock,
    tap_mock2,
    test_config,
):
    # tap_mock2 is needed by the data_source_composable specialization
    # this test is about making sure the summary .txt files are copied
    # as expected when there is more than one time-box
    pattern = None
    try:
        clients_mock.return_value.metadata_client.read.side_effect = Mock(
            return_value=SimpleObservation(
                collection=test_config.collection,
                observation_id='def',
                algorithm=Algorithm('test'),
            )
        )
        visit_meta_mock.side_effect = _mock_visit
        tap_mock.side_effect = _mock_get_work

        test_end_time = datetime.fromtimestamp(1579740838)
        start_time = test_end_time - timedelta(seconds=900)
        _write_state(start_time)

        test_config.task_types = [mc.TaskType.INGEST]
        test_config.log_to_file = True
        test_config.state_fqn = STATE_FILE
        test_config.interval = 10
        pattern = f'{test_config.success_fqn.split(".")[0]}*'

        if os.path.exists(test_config.progress_fqn):
            os.unlink(test_config.progress_fqn)

        # preconditions for the success file: - one file named pattern.txt
        #
        original_success_files = glob.glob(pattern)
        for entry in original_success_files:
            os.unlink(entry)
        if not os.path.exists(test_config.success_fqn):
            with open(test_config.success_fqn, 'w') as f:
                f.write('test content\n')

        test_chooser = ec.OrganizeChooser()
        test_result = rc.run_by_state(
            config=test_config,
            chooser=test_chooser,
            bookmark_name=TEST_BOOKMARK,
            end_time=test_end_time,
        )
        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'expect success'
        assert os.path.exists(test_config.progress_fqn), 'expect progress file'
        file_count = glob.glob(pattern)
        assert len(file_count) == 2, 'wrong number of success files'
    finally:
        if pattern is not None:
            original_success_files = glob.glob(pattern)
            for entry in original_success_files:
                os.unlink(entry)


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_todo_list_dir_data_source_exception(
    do_one_mock, clients_mock, test_config
):
    test_config.working_directory = TEST_DIR
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.log_to_file = True

    for entry in [False, True]:
        test_config.features.supports_latest_client = entry
        do_one_mock.side_effect = mc.CadcException

        if os.path.exists(test_config.failure_fqn):
            os.unlink(test_config.failure_fqn)
        if os.path.exists(test_config.retry_fqn):
            os.unlink(test_config.retry_fqn)

        test_chooser = ec.OrganizeChooser()
        test_data_source = dsc.ListDirDataSource(test_config, test_chooser)
        test_result = rc.run_by_todo(
            config=test_config,
            chooser=test_chooser,
            source=test_data_source,
        )
        assert test_result is not None, 'expect a result'
        assert test_result == -1, 'expect failure'
        assert do_one_mock.called, 'expect do_one call'
        assert os.path.exists(test_config.failure_fqn), 'expect failure file'
        assert os.path.exists(test_config.retry_fqn), 'expect retry file'

        with open(test_config.failure_fqn) as f:
            content = f.read()
            # the obs id and file name
            assert 'abc abc.fits' in content, 'wrong failure message'

        with open(test_config.retry_fqn) as f:
            content = f.read()
            # retry file names
            assert content == 'abc.fits\n', 'wrong retry content'

        assert not (
            clients_mock.return_value.metadata_client.read.called
        ), 'scrape, should be no metadata client call'
        assert not (
            clients_mock.return_value.data_client.get_file_info.called
        ), 'scrape, should be no data client call'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_todo_retry(do_one_mock, clients_mock, test_config):
    test_config.features.supports_latest_client = True
    (
        retry_success_fqn,
        retry_failure_fqn,
        retry_retry_fqn,
    ) = _clean_up_log_files(test_config)

    do_one_mock.side_effect = _mock_do_one

    test_config.work_fqn = f'{tc.TEST_DATA_DIR}/todo.txt'
    test_config.log_to_file = True
    test_config.retry_failures = True
    _write_todo(test_config)

    test_result = rc.run_by_todo(config=test_config)

    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure'
    _check_log_files(
        test_config, retry_success_fqn, retry_failure_fqn, retry_retry_fqn
    )
    assert do_one_mock.called, 'expect do_one call'
    assert do_one_mock.call_count == 2, 'wrong number of calls'

    assert not (
        clients_mock.return_value.metadata_client.read.called
    ), 'do_one is mocked, should be no metadata client call'
    assert not (
        clients_mock.return_value.data_client.get_file_info.called
    ), 'do_one is mocked, should be no data client call'


@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
@patch('caom2pipe.data_source_composable.CadcTapClient')
@patch(
    'caom2pipe.data_source_composable.QueryTimeBoxDataSource.'
    'get_time_box_work'
)
@pytest.mark.skip('')
def test_run_state_retry(get_work_mock, tap_mock, do_one_mock, test_config):
    _write_state(rc.get_utc_now_tz())
    (
        retry_success_fqn,
        retry_failure_fqn,
        retry_retry_fqn,
    ) = _clean_up_log_files(test_config)
    global call_count
    call_count = 0
    # get_work_mock.return_value.get_time_box_work.side_effect = _mock_get_work
    get_work_mock.side_effect = _mock_get_work
    do_one_mock.side_effect = _mock_do_one

    test_config.log_to_file = True
    test_config.retry_failures = True
    test_config.state_fqn = STATE_FILE
    test_config.interval = 10
    test_config.logging_level = 'DEBUG'

    test_result = rc.run_by_state(
        config=test_config,
        command_name=TEST_COMMAND,
        bookmark_name=TEST_BOOKMARK,
    )

    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure'
    _check_log_files(
        test_config,
        retry_success_fqn,
        retry_failure_fqn,
        retry_retry_fqn,
    )
    assert do_one_mock.called, 'expect do_one call'
    assert do_one_mock.call_count == 2, 'wrong number of calls'
    assert tap_mock.called, 'init should be called'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_single(do_mock, get_access_mock, test_config):
    get_access_mock.return_value = 'https://localhost'
    _clean_up_log_files(test_config)
    progress_file = os.path.join(tc.TEST_DATA_DIR, 'progress.txt')

    test_config.features.expects_retry = False
    test_config.progress_fqn = progress_file

    test_config.state_fqn = STATE_FILE
    test_config.interval = 5
    test_state = mc.State(test_config.state_fqn)
    test_state.save_state('gemini_timestamp', datetime.utcnow())

    do_mock.return_value = -1

    test_url = 'http://localhost/test_url.fits'
    test_storage_name = mc.StorageName(url=test_url)

    test_result = rc.run_single(
        test_config,
        test_storage_name,
        meta_visitors=None,
        data_visitors=None,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'wrong result'

    assert do_mock.called, 'do mock not called'
    assert do_mock.call_count == 1, do_mock.call_count
    args, kwargs = do_mock.call_args
    test_storage = args[0]
    assert isinstance(test_storage, mc.StorageName), type(test_storage)
    assert test_storage.obs_id is None, 'wrong obs id'
    assert test_storage.url == test_url, test_storage.url


# TODO - make this work with TodoRunner AND StateRunner
# for the 'finish_run' call
@pytest.mark.skip('')
def test_capture_failure(test_config):
    start_s = datetime.utcnow().timestamp()
    test_obs_id = 'test_obs_id'
    test_obs_id_2 = 'test_obs_id_2'
    log_file_directory = os.path.join(tc.THIS_DIR, 'logs')
    test_config.log_to_file = True
    test_config.log_file_directory = log_file_directory
    success_log_file_name = 'success_log.txt'
    test_config.success_log_file_name = success_log_file_name
    failure_log_file_name = 'failure_log.txt'
    test_config.failure_log_file_name = failure_log_file_name
    retry_file_name = 'retries.txt'
    test_config.retry_file_name = retry_file_name
    rejected_file_name = 'rejected.yml'
    test_config.rejected_file_name = rejected_file_name

    # clean up from last execution
    if not os.path.exists(log_file_directory):
        os.mkdir(log_file_directory)
    if os.path.exists(test_config.success_fqn):
        os.remove(test_config.success_fqn)
    if os.path.exists(test_config.failure_fqn):
        os.remove(test_config.failure_fqn)
    if os.path.exists(test_config.retry_fqn):
        os.remove(test_config.retry_fqn)
    if os.path.exists(test_config.rejected_fqn):
        os.remove(test_config.rejected_fqn)

    test_oe = ec.OrganizeExecutesWithDoOne(test_config, 'command', [], [])
    test_sname = tc.TestStorageName(obs_id=test_obs_id_2)
    test_oe.capture_failure(test_sname, 'Cannot build an observation')
    test_sname = tc.TestStorageName(obs_id=test_obs_id)
    test_oe.capture_failure(test_sname, 'exception text')
    test_oe.capture_success(test_obs_id, 'C121212_01234_CAL.fits.gz', start_s)
    test_oe.finish_run(test_config)

    assert os.path.exists(test_config.success_fqn)
    assert os.path.exists(test_config.failure_fqn)
    assert os.path.exists(test_config.retry_fqn)
    assert os.path.exists(test_config.rejected_fqn)

    success_content = open(test_config.success_fqn).read()
    assert (
        'test_obs_id C121212_01234_CAL.fits.gz' in success_content
    ), 'wrong content'
    retry_content = open(test_config.retry_fqn).read()
    assert retry_content == 'test_obs_id\n'
    failure_content = open(test_config.failure_fqn).read()
    assert failure_content.endswith(
        'Unknown error. Check specific log.\n'
    ), failure_content
    assert os.path.exists(test_config.rejected_fqn), test_config.rejected_fqn
    rejected_content = mc.read_as_yaml(test_config.rejected_fqn)
    assert rejected_content is not None, 'expect a result'
    test_result = rejected_content.get('bad_metadata')
    assert test_result is not None, 'wrong result'
    assert len(test_result) == 1, 'wrong number of entries'
    assert test_result[0] == test_obs_id, 'wrong entry'


# TODO make these into useful tests somewhere
@pytest.mark.skip('')
def test_time_box(test_config):
    _write_state('23-Jul-2019 09:51')
    test_config.state_fqn = STATE_FILE
    test_config.interval = 700

    # class MakeWork(mc.Work):
    #
    #     def __init__(self):
    #         super(MakeWork, self).__init__(
    #             mc.make_seconds('24-Jul-2019 09:20'))
    #         self.todo_call_count = 0
    #         self.zero_called = False
    #         self.one_called = False
    #         self.two_called = False
    #
    #     def initialize(self):
    #         pass
    #
    #     def todo(self, prev_exec_date, exec_date):
    #         if self.todo_call_count == 0:
    #             assert prev_exec_date == datetime(2019, 7, 23, 9, 51), \
    #                 'wrong prev'
    #             assert exec_date == datetime(2019, 7, 23, 21, 31), 'wrong exec'
    #             self.zero_called = True
    #         elif self.todo_call_count == 1:
    #             assert prev_exec_date == datetime(2019, 7, 23, 21, 31), \
    #                 'wrong prev'
    #             assert exec_date == datetime(2019, 7, 24, 9, 11), 'wrong exec'
    #             self.one_called = True
    #         elif self.todo_call_count == 2:
    #             assert prev_exec_date == datetime(2019, 7, 24, 9, 11), \
    #                 'wrong exec'
    #             assert exec_date == datetime(2019, 7, 24, 9, 20), 'wrong exec'
    #             self.two_called = True
    #         self.todo_call_count += 1
    #         assert self.todo_call_count <= 4, 'loop is written wrong'
    #         return []
    #
    # test_work = MakeWork()
    #
    # test_result = ec.run_from_state(test_config,
    #                                 sname=mc.StorageName,
    #                                 command_name=COMMAND_NAME,
    #                                 meta_visitors=None,
    #                                 data_visitors=None,
    #                                 bookmark_name=TEST_BOOKMARK,
    #                                 work=test_work)
    # assert test_result is not None, 'expect a result'
    #
    # test_state = mc.State(test_config.state_fqn)
    # assert test_work.zero_called, 'missed zero'
    # assert test_work.one_called, 'missed one'
    # assert test_work.two_called, 'missed two'
    # assert test_state.get_bookmark(TEST_BOOKMARK) == \
    #     datetime(2019, 7, 24, 9, 20)
    # assert test_work.todo_call_count == 3, 'wrong todo call count'


@pytest.mark.skip('')
def test_time_box_equal(test_config):
    _write_state('23-Jul-2019 09:51')
    test_config.state_fqn = STATE_FILE
    test_config.interval = 700

    # class MakeWork(mc.Work):
    #
    #     def __init__(self):
    #         super(MakeWork, self).__init__(
    #             mc.make_seconds('23-Jul-2019 09:51'))
    #         self.todo_call_count = 0
    #         self.zero_called = False
    #         self.one_called = False
    #         self.two_called = False
    #
    #     def initialize(self):
    #         pass
    #
    #     def todo(self, prev_exec_date, exec_date):
    #         self.todo_call_count += 1
    #         assert self.todo_call_count <= 4, 'loop is written wrong'
    #         return []
    #
    # test_work = MakeWork()
    #
    # test_result = ec.run_from_state(test_config,
    #                                 sname=mc.StorageName,
    #                                 command_name=COMMAND_NAME,
    #                                 meta_visitors=None,
    #                                 data_visitors=None,
    #                                 bookmark_name=TEST_BOOKMARK,
    #                                 work=test_work)
    # assert test_result is not None, 'expect a result'
    # test_state = mc.State(test_config.state_fqn)
    # assert test_state.get_bookmark(TEST_BOOKMARK) == \
    #     datetime(2019, 7, 23, 9, 51)
    # assert test_work.todo_call_count == 0, 'wrong todo call count'


@pytest.mark.skip('')
def test_time_box_once_through(test_config):
    _write_state('23-Jul-2019 09:51')
    test_config.state_fqn = STATE_FILE
    test_config.interval = 700

    # class MakeWork(mc.Work):
    #
    #     def __init__(self):
    #         super(MakeWork, self).__init__(
    #             mc.make_seconds('23-Jul-2019 12:20'))
    #         self.todo_call_count = 0
    #         self.zero_called = False
    #
    #     def initialize(self):
    #         pass
    #
    #     def todo(self, prev_exec_date, exec_date):
    #         if self.todo_call_count == 0:
    #             assert prev_exec_date == datetime(2019, 7, 23, 9, 51), \
    #                 'wrong prev'
    #             assert exec_date == datetime(2019, 7, 23, 12, 20), 'wrong exec'
    #             self.zero_called = True
    #         self.todo_call_count += 1
    #         assert self.todo_call_count <= 4, 'loop is written wrong'
    #         return []
    #
    # test_work = MakeWork()
    #
    # test_result = ec.run_from_state(test_config,
    #                                 sname=mc.StorageName,
    #                                 command_name=COMMAND_NAME,
    #                                 meta_visitors=None,
    #                                 data_visitors=None,
    #                                 bookmark_name=TEST_BOOKMARK,
    #                                 work=test_work)
    # assert test_result is not None, 'expect a result'
    #
    # test_state = mc.State(test_config.state_fqn)
    # assert test_work.zero_called, 'missed zero'
    # assert test_state.get_bookmark(TEST_BOOKMARK) == \
    #     datetime(2019, 7, 23, 12, 20)
    # assert test_work.todo_call_count == 1, 'wrong todo call count'


def _clean_up_log_files(test_config):
    retry_success_fqn = (
        f'{tc.TEST_DATA_DIR}_0/' f'{test_config.success_log_file_name}'
    )
    retry_failure_fqn = (
        f'{tc.TEST_DATA_DIR}_0/' f'{test_config.failure_log_file_name}'
    )
    retry_retry_fqn = f'{tc.TEST_DATA_DIR}_0/{test_config.retry_file_name}'
    for ii in [
        test_config.success_fqn,
        test_config.failure_fqn,
        test_config.retry_fqn,
        retry_failure_fqn,
        retry_retry_fqn,
        retry_success_fqn,
    ]:
        if os.path.exists(ii):
            os.unlink(ii)
    return retry_success_fqn, retry_failure_fqn, retry_retry_fqn


def _check_log_files(
    test_config, retry_success_fqn, retry_failure_fqn, retry_retry_fqn
):
    assert os.path.exists(test_config.success_fqn), 'empty success file'
    success_size = mc.get_file_size(test_config.success_fqn)
    assert success_size == 0, 'empty success file'
    assert os.path.exists(test_config.failure_fqn), 'expect failure file'
    assert os.path.exists(test_config.retry_fqn), 'expect retry file'
    assert os.path.exists(retry_success_fqn), 'empty success file'
    success_size = mc.get_file_size(retry_success_fqn)
    assert success_size == 0, 'empty success file'
    assert os.path.exists(retry_failure_fqn), 'expect failure file'
    assert os.path.exists(retry_retry_fqn), 'expect retry file'


def _write_state(start_time):
    if os.path.exists(STATE_FILE):
        os.unlink(STATE_FILE)
    test_bookmark = {
        'bookmarks': {
            TEST_BOOKMARK: {
                'last_record': start_time,
            },
        },
    }
    mc.write_as_yaml(test_bookmark, STATE_FILE)


def _write_todo(test_config):
    with open(test_config.work_fqn, 'w') as f:
        f.write(f'test_obs_id.fits.gz')


call_count = 0


def _mock_get_work(arg1, arg2):
    return _mock_query(None, None, None)


def _mock_get_work2(arg1, **kwargs):
    return _mock_query(None, None, None)


def _mock_query(arg1, arg2, arg3):
    global call_count
    if call_count == 0:
        logging.error('returning results')
        call_count = 1
        return Table.read(
            'fileName,ingestDate\n'
            'NEOS_SCI_2015347000000_clean.fits,'
            '2019-10-23T16:27:19.000\n'.split('\n'),
            format='csv',
        )
    else:
        logging.error('returning empty list')
        return Table.read('fileName,ingestDate\n'.split('\n'), format='csv')


def _mock_do_one(arg1):
    assert isinstance(arg1, mc.StorageName), 'expect StorageName instance'
    if arg1.obs_id == 'TEST_OBS_ID':
        assert (
            arg1.lineage == 'TEST_OBS_ID/ad:OMM/TEST_OBS_ID.fits.gz'
        ), 'wrong lineage'
        assert arg1.file_name == 'TEST_OBS_ID.fits', 'wrong file name'
        with open(f'{tc.TEST_DATA_DIR}/retry.txt', 'w') as f:
            f.write(f'ghi.fits.gz')
    elif arg1.obs_id == 'ghi':
        assert arg1.lineage == 'ghi/ad:OMM/ghi.fits.gz', 'wrong lineage'
        assert arg1.file_name == 'ghi.fits', 'wrong file name'
    else:
        assert False, f'unexpected obs id {arg1.obs_id}'
    return -1


def _mock_write():
    fqn = (
        f'{tc.THIS_DIR}/NEOS_SCI_2015347000000_clean/'
        f'NEOS_SCI_2015347000000_clean.xml'
    )
    mc.write_obs_to_file(
        SimpleObservation(
            collection='test_collection',
            observation_id='ghi',
            algorithm=Algorithm('test'),
        ),
        fqn,
    )


def _mock_read2(ign1, ign2):
    return _mock_read(None)


def _mock_read(ignore_fqn):
    return SimpleObservation(
        collection='test_collection',
        observation_id='ghi',
        algorithm=Algorithm('test'),
    )


def _mock_visit():
    return SimpleObservation(
        collection='test_collection',
        observation_id='ghi',
        algorithm=Algorithm('test'),
    )

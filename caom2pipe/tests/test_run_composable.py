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

import pytest
import glob
import os

from astropy.table import Table
from collections import deque
from datetime import datetime, timedelta, timezone

from unittest.mock import Mock, patch, call
import test_conf as tc

from cadcdata import FileInfo
from cadcutils import exceptions
from caom2 import SimpleObservation, Algorithm
from caom2pipe.client_composable import ClientCollection
from caom2pipe import data_source_composable as dsc
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from caom2pipe.reader_composable import FileMetadataReader
from caom2pipe import run_composable as rc
from caom2pipe import name_builder_composable as b


STATE_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_state.yml')
TEST_BOOKMARK = 'test_bookmark'
TEST_COMMAND = 'test_command'
TEST_DIR = f'{tc.TEST_DATA_DIR}/run_composable'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.manage_composable.read_obs_from_file')
@patch('caom2pipe.manage_composable.write_obs_to_file')
def test_run_todo_list_dir_data_source(
    write_obs_mock,
    read_obs_mock,
    visit_meta_mock,
    clients_mock,
    test_config,
    tmpdir,
):
    read_obs_mock.side_effect = _mock_read
    test_config.change_working_directory(tmpdir)
    test_config.use_local_files = True
    test_config.data_sources = ['/test_files/sub_directory']
    test_config.data_source_extensions = ['.fits']
    test_config.task_types = [mc.TaskType.SCRAPE]
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
def test_run_todo_list_dir_data_source_invalid_fname(clients_mock, test_config, tmpdir):
    test_fqn = os.path.join(tmpdir, 'abc.fits.gz')
    with open(test_fqn, 'w') as f:
        f.write('abc')

    test_config.change_working_directory(tmpdir)
    test_config.use_local_files = True
    test_config.data_sources = [tmpdir]
    test_config.data_source_extensions = ['.fits', '.fits.gz']
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.log_to_file = False

    class TestStorageName(mc.StorageName):
        def __init__(self, entry):
            super().__init__(obs_id=os.path.basename(entry))
            self._source_names = [entry]

        def is_valid(self):
            return False

    class TestStorageNameInstanceBuilder(b.StorageNameInstanceBuilder):
        def __init__(self):
            pass

        def build(self, entry):
            return TestStorageName(entry)

    test_builder = TestStorageNameInstanceBuilder()
    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config, chooser=test_chooser, name_builder=test_builder)
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


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_todo_file_data_source(clients_mock, test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_config.proxy_fqn = 'test_proxy.pem'
    clients_mock.return_value.metadata_client.read.side_effect = Mock(
        return_value=SimpleObservation(
            collection=test_config.collection,
            observation_id='def',
            algorithm=Algorithm('test'),
        )
    )

    test_config.work_fqn = f'{tmpdir}/todo.txt'
    with open(test_config.work_fqn, 'w') as f:
        f.write('def.fits.gz\n')

    test_config.task_types = [mc.TaskType.VISIT]
    test_config.log_to_file = True
    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config, chooser=test_chooser)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert os.path.exists(test_config.success_fqn), 'expect success file'

    with open(test_config.success_fqn) as f:
        content = f.read()
        # the obs id and file name
        assert 'def def.fits' in content, 'wrong success message'
    assert clients_mock.return_value.metadata_client.read.called, 'expect e call'
    clients_mock.return_value.metadata_client.read.assert_called_with('OMM', 'def'), 'wrong e args'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.data_source_composable.CadcTapClient')
@patch('caom2pipe.client_composable.query_tap_client')
@patch('caom2pipe.execute_composable.MetaVisit._visit_meta')
def test_run_state(
    visit_meta_mock,
    tap_query_mock,
    tap_mock,
    clients_mock,
    test_config,
    tmpdir,
):
    # tap_mock is used by the data_source_composable class
    visit_meta_mock.side_effect = _mock_visit
    clients_mock.return_value.metadata_client.read.side_effect = _mock_read2
    tap_query_mock.side_effect = _mock_get_work

    test_end_time = datetime.fromtimestamp(1579740838, tz=timezone.utc)
    start_time = test_end_time - timedelta(seconds=900)

    test_config.task_types = [mc.TaskType.INGEST]
    test_config.interval = 10
    individual_log_file = (
        f'{test_config.log_file_directory}/NEOS_SCI_2015347000000_clean.log'
    )

    test_config.change_working_directory(tmpdir)
    test_config.state_file_name = 'state.yml'
    _write_state(start_time, test_config.state_fqn)

    test_chooser = ec.OrganizeChooser()
    # use_local_files set so run_by_state chooses QueryTimeBoxDataSource
    test_config.use_local_files = False
    test_reader = Mock()
    test_result = rc.run_by_state(
        config=test_config,
        chooser=test_chooser,
        bookmark_name=TEST_BOOKMARK,
        end_time=test_end_time,
        metadata_reader=test_reader,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert visit_meta_mock.called, 'expect visit meta call'
    visit_meta_mock.assert_called_once_with()
    assert test_reader.reset.called, 'expect reset call'
    assert test_reader.reset.call_count == 1, 'wrong call count'

    test_state = mc.State(test_config.state_fqn)
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
    _write_state(start_time, test_config.state_fqn)
    visit_meta_mock.reset_mock()
    test_result = rc.run_by_state(
        config=test_config,
        chooser=test_chooser,
        bookmark_name=TEST_BOOKMARK,
        end_time=test_end_time,
        metadata_reader=test_reader,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert not visit_meta_mock.called, 'expect no visit_meta call'
    assert test_reader.reset.called, 'expect reset call'
    assert test_reader.reset.call_count == 1, 'wrong call count'


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
        assert os.path.exists(
            test_config.progress_fqn
        ), 'expect progress file'
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
    do_one_mock, clients_mock, test_config, tmpdir
):
    test_config.change_working_directory(tmpdir)
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.log_to_file = True

    with open(f'{tmpdir}/abc.fits.gz', 'w') as f:
        f.write('test content\n')

    do_one_mock.side_effect = mc.CadcException

    test_chooser = ec.OrganizeChooser()
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True), application='DEFAULT')
    test_data_source = dsc.ListDirDataSource(test_config, test_chooser)
    test_data_source.reporter = test_reporter
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


@patch('caom2pipe.data_source_composable.TodoFileDataSource.clean_up')
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_todo_retry(do_one_mock, clients_mock, source_mock, test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_config.log_to_file = True
    test_config.retry_failures = True
    test_config.retry_decay = 0
    _write_todo(test_config)
    retry_success_fqn = f'{tmpdir}/logs_0/' f'{test_config.success_log_file_name}'
    retry_failure_fqn = f'{tmpdir}/logs_0/' f'{test_config.failure_log_file_name}'
    retry_retry_fqn = f'{tmpdir}/logs_0/{test_config.retry_file_name}'
    do_one_mock.side_effect = _mock_do_one

    test_result = rc.run_by_todo(config=test_config)

    # what should happen when a failure execution occurs
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
    assert source_mock.called, 'clean_up should be called'
    assert source_mock.call_count == 2, 'clean_up should be called two times'
    calls = [
        call('test_obs_id.fits.gz', -1, 0),
        call('test_obs_id.fits.gz', -1, 1),
    ]
    source_mock.assert_has_calls(calls)

    # what should happen when successful execution occurs
    source_mock.reset_mock()
    do_one_mock.reset_mock()
    do_one_mock.side_effect = None
    do_one_mock.return_value = 0
    test_result = rc.run_by_todo(config=test_config)

    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    _check_log_files(
        test_config, retry_success_fqn, retry_failure_fqn, retry_retry_fqn
    )
    assert do_one_mock.called, 'expect do_one call'
    assert do_one_mock.call_count == 1, 'wrong number of calls'
    source_mock.assert_called_with('test_obs_id.fits.gz', 0, 0)


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


@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.reader_composable.FileMetadataReader._retrieve_headers')
@patch('caom2pipe.reader_composable.FileMetadataReader._retrieve_file_info')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._move_action')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource.get_work')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_store_ingest_failure(
    data_client_mock,
    repo_client_mock,
    get_work_mock,
    cleanup_mock,
    reader_file_info_mock,
    reader_headers_mock,
    access_mock,
    visit_meta_mock,
    caom2_store_mock,
    test_config,
    tmpdir,
):
    access_mock.return_value = 'https://localhost'
    temp_deque = deque()
    temp_deque.append('/data/dao_c122_2021_005157_e.fits')
    temp_deque.append('/data/dao_c122_2021_005157.fits')
    get_work_mock.return_value = temp_deque
    repo_client_mock.return_value.read.return_value = None
    reader_headers_mock.return_value = [{'OBSMODE': 'abc'}]

    def _file_info_mock(key, ignore):
        return FileInfo(
            id=key,
            file_type='application/fits',
            md5sum='md5:def',
        )

    # this is the exception raised in data_util.StorageClientWrapper, as
    # the mc.CadcException definition is not available to that package
    data_client_mock.return_value.put.side_effect = (
        exceptions.UnexpectedException
    )
    # mock a series of failures with the CADC storage service
    data_client_mock.return_value.info.side_effect = (
        exceptions.UnexpectedException
    )
    reader_file_info_mock.side_effect = _file_info_mock
    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.use_local_files = True
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.data_sources = ['/data']
    test_config.data_source_extensions = ['.fits']
    test_config.logging_level = 'INFO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.write_to_file(test_config)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    data_source = dsc.LocalFilesDataSource(
        test_config,
        data_client_mock,
        Mock(),
    )
    test_result = rc.run_by_todo(source=data_source)
    assert test_result is not None, 'expect result'
    assert test_result == -1, 'expect failure'
    # execution stops before this call should be made
    assert not repo_client_mock.return_value.read.called, 'no read'
    # make sure data is not really being written to CADC storage :)
    assert (
        data_client_mock.return_value.put.called
    ), 'put should be called'
    assert (
        data_client_mock.return_value.put.call_count == 2
    ), 'wrong number of puts'
    put_calls = [
        call('/data', 'cadc:OMM/dao_c122_2021_005157_e.fits'),
        call('/data', 'cadc:OMM/dao_c122_2021_005157.fits'),
    ]
    data_client_mock.return_value.put.assert_has_calls(
        put_calls, any_order=False
    )
    assert cleanup_mock.called, 'cleanup'
    cleanup_calls = [
        call('/data/dao_c122_2021_005157_e.fits', '/data/failure'),
        call('/data/dao_c122_2021_005157.fits', '/data/failure'),
    ]
    cleanup_mock.assert_has_calls(cleanup_calls), 'wrong cleanup args'
    assert not visit_meta_mock.called, 'no _visit_meta call'
    assert not caom2_store_mock.called, 'no _caom2_store call'
    assert reader_file_info_mock.called, 'info'
    reader_file_info_mock.assert_called_with('cadc:OMM/dao_c122_2021_005157.fits', '/data/dao_c122_2021_005157.fits'), 'info args'
    assert reader_headers_mock.called, 'get_head should be called'
    reader_headers_mock.assert_called_with('cadc:OMM/dao_c122_2021_005157.fits', '/data/dao_c122_2021_005157.fits'), 'headers mock call'


@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._move_action')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_store_get_work_failures(
    data_client_mock,
    repo_client_mock,
    cleanup_mock,
    access_mock,
    visit_meta_mock,
    caom2_store_mock,
    test_config,
    tmpdir,
):
    # this is a test that a fitsverify failure will be logged in the failure log,
    # and an md5sum that is the same on disk as at CADC will be logged in the success log

    access_mock.return_value = 'https://localhost'
    repo_client_mock.return_value.read.return_value = None

    def _file_info_mock(uri):
        return FileInfo(
            id=uri,
            file_type='application/fits',
            md5sum='md5:d937df477fe2511995fa39a027e8ce2f',
        )

    data_client_mock.info.side_effect = _file_info_mock
    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.use_local_files = True
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.store_modified_files_only = True
    test_config.data_sources = ['/data']
    test_config.data_source_extensions = ['.fits']
    test_config.logging_level = 'INFO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.recurse_data_sources = False
    test_config.write_to_file(test_config)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    test_end_time = datetime.fromtimestamp(1579740838, tz=timezone.utc)
    start_time = test_end_time - timedelta(seconds=900)
    _write_state(start_time, test_config.state_fqn)

    stat_return_value = type('', (), {})
    stat_return_value.st_mtime = 1579740835.7357888
    dir_entry_1 = type('', (), {})
    dir_entry_1.name = 'a2020_06_17_07_00_01.fits'
    dir_entry_1.path = '/test_files/a2020_06_17_07_00_01.fits'
    dir_entry_1.stat = Mock(return_value=stat_return_value)
    dir_entry_1.is_dir = Mock(return_value=False)
    dir_entry_2 = type('', (), {})
    dir_entry_2.name = 'a2022_07_26_05_50_01.fits'
    dir_entry_2.path = '/test_files/a2022_07_26_05_50_01.fits'
    dir_entry_2.stat = Mock(return_value=stat_return_value)
    dir_entry_2.is_dir = Mock(return_value=False)

    file_metadata_reader = FileMetadataReader()
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True), application='DEFAULT')
    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = [dir_entry_1, dir_entry_2]
        data_source = dsc.LocalFilesDataSource(test_config, data_client_mock, file_metadata_reader)
        data_source.reporter = test_reporter
        clients_mock = ClientCollection(test_config)
        clients_mock._data_client = data_client_mock
        clients_mock._metadata_client = repo_client_mock
        test_result = rc.run_by_state(
            source=data_source,
            bookmark_name=TEST_BOOKMARK,
            end_time=test_end_time,
            clients=clients_mock,
            metadata_reader=file_metadata_reader,
        )

        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect successful execution'
        # execution stops before this call should be made
        assert not repo_client_mock.return_value.read.called, 'no read'
        # make sure data is not really being written to CADC storage :)
        assert not data_client_mock.put.called, 'put should not be called'
        assert cleanup_mock.called, 'cleanup'
        cleanup_calls = [
            call('/test_files/a2020_06_17_07_00_01.fits', '/data/success'),
            call('/test_files/a2022_07_26_05_50_01.fits', '/data/failure'),
        ]
        cleanup_mock.assert_has_calls(cleanup_calls), 'wrong cleanup args'
        assert not visit_meta_mock.called, 'no _visit_meta call'
        assert not caom2_store_mock.called, 'no _caom2_store call'
        assert os.path.exists(test_config.failure_fqn), f'failure log {test_config.failure_fqn} should exist'
        with open(test_config.failure_fqn, 'r') as f:
            content = f.readlines()
            assert len(content) == 1, 'expect 1 failure'
            assert 'a2022_07_26_05_50_01' in content[0], 'expect verify in failure log'
            assert 'a2020_06_17_07_00_01' not in content[0], 'expect md5sum in success log'

        assert os.path.exists(test_config.success_fqn), f'failure log {test_config.success_fqn} should exist'
        with open(test_config.success_fqn, 'r') as f:
            content = f.readlines()
            assert len(content) == 1, 'expect 1 success'
            assert 'a2020_06_17_07_00_01' in content[0], 'expect md5sum in success log'
            assert 'a2022_07_26_05_50_01' not in content[0], 'expect verify in failure log'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.data_source_composable.TodoFileDataSource.get_work')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_ingest(
    data_client_mock,
    repo_client_mock,
    data_source_mock,
    meta_visit_mock,
    caom2_store_mock,
    access_url_mock,
    test_config,
    tmpdir
):
    access_url_mock.return_value = 'https://localhost:8080'
    temp_deque = deque()
    test_f_name = '1319558w.fits.fz'
    temp_deque.append(test_f_name)
    data_source_mock.return_value = temp_deque
    repo_client_mock.return_value.read.return_value = None
    data_client_mock.return_value.get_head.return_value = [
        {'INSTRUME': 'WIRCam'},
    ]

    data_client_mock.return_value.info.return_value = FileInfo(
        id=test_f_name,
        file_type='application/fits',
        md5sum='abcdef',
    )

    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.logging_level = 'INFO'
    test_config.collection = 'CFHT'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.use_local_files = False
    test_config.write_to_file(test_config)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True), application='DEFAULT')
    test_data_source = dsc.TodoFileDataSource(test_config)
    test_data_source.reporter = test_reporter
    test_result = rc.run_by_todo(source=test_data_source)
    assert test_result is not None, 'expect result'
    assert test_result == 0, 'expect success'
    assert repo_client_mock.return_value.read.called, 'read called'
    assert data_client_mock.return_value.info.called, 'info'
    assert (
        data_client_mock.return_value.info.call_count == 1
    ), 'wrong number of info calls'
    data_client_mock.return_value.info.assert_called_with(f'cadc:CFHT/{test_f_name}')
    assert (
        data_client_mock.return_value.get_head.called
    ), 'get_head should be called'
    assert (
        data_client_mock.return_value.get_head.call_count == 1
    ), 'wrong number of get_heads'
    data_client_mock.return_value.get_head.assert_called_with(f'cadc:CFHT/{test_f_name}')
    assert meta_visit_mock.called, '_visit_meta call'
    assert meta_visit_mock.call_count == 1, '_visit_meta call count'
    assert caom2_store_mock.called, '_caom2_store call'
    assert caom2_store_mock.call_count == 1, '_caom2_store call count'
    # get_work is mocked, so Reporter/Summary cannot be checked


@patch('caom2pipe.execute_composable.get_local_file_info')
@patch('caom2pipe.client_composable.vault_info')
@patch('caom2pipe.execute_composable.FitsForCADCDecompressor.fix_compression')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('vos.vos.Client')
def test_vo_with_cleanup(
    vo_client_mock,
    access_mock,
    fix_mock,
    vault_info_mock,
    local_file_info_mock,
    test_config,
    tmpdir
):
    access_mock.return_value = 'https://localhost'
    test_obs_id = 'sky_cam_image'
    test_f_name = f'{test_obs_id}.fits.gz'
    vo_client_mock.listdir.return_value = ['sky_cam_image.fits.gz']
    vo_client_mock.isdir.return_value = False
    vo_client_mock.status.return_value = False
    test_file_info = FileInfo(
        id=test_f_name,
        file_type='application/fits',
        md5sum='abcdef',
    )
    vault_info_mock.return_value = test_file_info
    local_file_info_mock.return_value = test_file_info
    clients_mock = Mock()
    clients_mock.vo_client = vo_client_mock
    clients_mock.data_client.info.side_effect = [None, test_file_info]
    store_transfer_mock = Mock(autospec=True)
    fix_mock.return_value = f'{tmpdir}/{test_f_name}'
    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE]
    test_config.logging_level = 'INFO'
    test_config.collection = 'DAO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.data_sources = ['vos:goliaths/DAOTest']
    test_config.data_source_extensions = ['.fits.gz']
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = 'vos:goliaths/DAOTest/fail'
    test_config.cleanup_success_destination = 'vos:goliaths/DAOTest/pass'
    test_config.store_modified_files_only = True
    test_config.write_to_file(test_config)
    test_builder = nbc.GuessingBuilder(mc.StorageName)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    # execution
    test_result = rc.run_by_todo(
        clients=clients_mock,
        store_transfer=store_transfer_mock,
        name_builder=test_builder,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert fix_mock.called, 'fix_compression call'
    fix_mock.assert_called_with(f'{tmpdir}/sky_cam_image/{test_f_name}')
    assert vo_client_mock.move.called, 'vo mock call'
    vo_client_mock.move.assert_called_with(
        'vos:goliaths/DAOTest/sky_cam_image.fits.gz',
        'vos:goliaths/DAOTest/pass/sky_cam_image.fits.gz',
    ), 'move args'
    assert clients_mock.data_client.put.called, 'put call'
    clients_mock.data_client.put.assert_called_with(tmpdir, 'cadc:DAO/sky_cam_image.fits')


@patch('caom2pipe.data_source_composable.TodoFileDataSource.get_work')
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_store_from_to_cadc(clients_mock, get_work_mock, test_config):
    # mimic a decompression event
    test_f_name = 'abc.fits'
    test_config.task_types = [mc.TaskType.STORE]
    test_config.logging_level = 'DEBUG'
    test_return_value = deque()
    test_return_value.append(
        f'cadc:{test_config.collection}/{test_f_name}.gz',
    )
    get_work_mock.return_value = test_return_value
    clients_mock.return_value.data_client.get.side_effect = (
        _mock_get_compressed_file
    )
    test_builder = nbc.GuessingBuilder(mc.StorageName)
    test_result = rc.run_by_todo(
        test_config,
        test_builder,
    )

    assert test_result == 0, 'expect success'
    assert clients_mock.return_value.data_client.get.called, 'get call'
    assert clients_mock.return_value.data_client.put.called, 'put call'
    clients_mock.return_value.data_client.get.assert_called_with(
        '/usr/src/app/caom2pipe/caom2pipe/tests/abc',
        f'cadc:{test_config.collection}/{test_f_name}.gz',
    ), 'wrong get params'
    clients_mock.return_value.data_client.put.assert_called_with(
        '/usr/src/app/caom2pipe/caom2pipe/tests/abc',
        f'cadc:{test_config.collection}/{test_f_name}',
    ), 'wrong put params'


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
    assert os.path.exists(retry_success_fqn), f'empty success file {retry_success_fqn}'
    success_size = mc.get_file_size(retry_success_fqn)
    assert success_size == 0, 'empty success file'
    assert os.path.exists(retry_failure_fqn), 'expect failure file'
    assert os.path.exists(retry_retry_fqn), 'expect retry file'


def _write_state(start_time, fqn=STATE_FILE):
    if os.path.exists(fqn):
        os.unlink(fqn)
    test_bookmark = {
        'bookmarks': {
            TEST_BOOKMARK: {
                'last_record': start_time,
            },
        },
    }
    mc.write_as_yaml(test_bookmark, fqn)


def _write_todo(test_config):
    with open(test_config.work_fqn, 'w') as f:
        f.write(f'test_obs_id.fits.gz')


call_count = 0


def _mock_get_compressed_file(working_dir, uri):
    fqn = f'{working_dir}/{os.path.basename(uri)}'
    with open(fqn, 'wb') as f:
        f.write(
            b"\x1f\x8b\x08\x08\xd0{Lb\x02\xff.abc.fits\x00+I-.QH\xce"
            b"\xcf+I\xcd+\xe1\x02\x00\xbd\xdfZ'\r\x00\x00\x00"
        )


def _mock_get_work(arg1, arg2):
    return _mock_query(None, None, None)


def _mock_get_work2(arg1, **kwargs):
    return _mock_query(None, None, None)


def _mock_query(arg1, arg2, arg3):
    global call_count
    if call_count == 0:
        call_count = 1
        return Table.read(
            'uri,lastModified\n'
            'cadc:NEOSSAT/NEOS_SCI_2015347000000_clean.fits,'
            '2019-10-23T16:27:19.000\n'.split('\n'),
            format='csv',
        )
    else:
        return Table.read('fileName,ingestDate\n'.split('\n'), format='csv')


def _mock_do_one(arg1):
    assert isinstance(arg1, mc.StorageName), 'expect StorageName instance'
    if arg1.obs_id == 'TEST_OBS_ID':
        assert arg1.file_name == 'TEST_OBS_ID.fits', 'wrong file name'
        with open(f'{tc.TEST_DATA_DIR}/retry.txt', 'w') as f:
            f.write(f'ghi.fits.gz')
    elif arg1.obs_id == 'ghi':
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

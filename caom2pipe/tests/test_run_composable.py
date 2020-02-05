# -*- coding: utf-8 -*-
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
import os

from astropy.table import Table
from cadctap import CadcTapClient
from datetime import datetime, timedelta

from mock import Mock, patch
import test_conf as tc

from caom2 import SimpleObservation, Algorithm
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import run_composable as rc
from caom2pipe import name_builder_composable as b


STATE_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_state.yml')
TEST_BOOKMARK = 'test_bookmark'
TEST_COMMAND = 'test_command'
TEST_DIR = f'{tc.TEST_DATA_DIR}/run_composable'
TEST_SOURCE = '{}/test_command/test_command.py'.format(
    distutils.sysconfig.get_python_lib())


@patch('caom2pipe.execute_composable.CaomExecute._fits2caom2_cmd_local_direct')
def test_run_todo_list_dir_data_source(fits2caom2_mock, test_config):
    test_config.working_directory = TEST_DIR
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config,
                                 chooser=test_chooser,
                                 command_name=TEST_COMMAND)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert fits2caom2_mock.called, 'expect fits2caom2 call'


def test_run_todo_list_dir_data_source_invalid_fname(test_config):
    test_config.working_directory = TEST_DIR
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.log_to_file = False

    if os.path.exists(test_config.failure_fqn):
        os.unlink(test_config.failure_fqn)
    if os.path.exists(test_config.retry_fqn):
        os.unlink(test_config.retry_fqn)

    class TestStorageName(mc.StorageName):
        def __init__(self, entry):
            self._obs_id = entry

        def is_valid(self):
            return False

    class TestStorageNameInstanceBuilder(b.StorageNameInstanceBuilder):
        def __init__(self):
            pass

        def build(self, entry):
            return TestStorageName(entry)

    test_builder = TestStorageNameInstanceBuilder()
    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config,
                                 chooser=test_chooser,
                                 name_builder=test_builder,
                                 command_name=TEST_COMMAND)
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure, because of file naming'
    assert not os.path.exists(test_config.failure_fqn), 'no logging, no ' \
                                                        'failure file'
    assert not os.path.exists(test_config.retry_fqn), 'no logging, no ' \
                                                      'retry file'
    test_config.log_to_file = True
    test_result = rc.run_by_todo(config=test_config,
                                 chooser=test_chooser,
                                 command_name=TEST_COMMAND)
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure, because of file naming'
    assert os.path.exists(test_config.failure_fqn), 'expect failure file'
    assert os.path.exists(test_config.retry_fqn), 'expect retry file'


@patch('caom2pipe.execute_composable.CaomExecute._repo_cmd_read_client')
@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.CAOM2RepoClient')
@patch('caom2pipe.execute_composable.CadcDataClient')
def test_run_todo_file_data_source(repo_get_mock, repo_mock, caps_mock,
                                   ad_mock, data_client_mock,
                                   test_config):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = [b'fileName\n']
    ad_mock.return_value.__enter__.return_value = response

    data_client_mock.return_value = SimpleObservation(
        collection=test_config.collection, observation_id='def',
        algorithm=Algorithm(str('test')))

    if os.path.exists(test_config.success_fqn):
        os.unlink(test_config.success_fqn)

    test_config.work_fqn = f'{TEST_DIR}/todo.txt'
    test_config.task_types = [mc.TaskType.VISIT]
    test_config.log_to_file = True

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config,
                                 chooser=test_chooser,
                                 command_name=TEST_COMMAND)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert os.path.exists(test_config.success_fqn), 'expect success file'

    with open(test_config.success_fqn) as f:
        content = f.read()
        # the obs id and file name
        assert 'def def.fits' in content, 'wrong success message'
    assert repo_mock.called, 'expect call'
    assert repo_get_mock.called, 'expect call'


@patch('caom2pipe.manage_composable.query_tap_client')
@patch('caom2pipe.execute_composable.CAOM2RepoClient')
@patch('caom2pipe.execute_composable.CadcDataClient')
@patch('caom2pipe.execute_composable.CaomExecute._fits2caom2_cmd_direct')
def test_run_state(fits2caom2_mock, data_mock, repo_mock, tap_mock,
                   test_config):
    fits2caom2_mock.side_effect = _mock_write
    data_mock.return_value.get_file_info.return_value = {'name':
                                                             'test_file.fits'}
    repo_mock.return_value.read.side_effect = Mock(return_value=None)
    tap_mock.side_effect = _mock_query
    CadcTapClient.__init__ = Mock(return_value=None)

    test_end_time = datetime.fromtimestamp(1579740838)
    start_time = test_end_time - timedelta(seconds=900)
    _write_state(start_time)

    test_config.task_types = [mc.TaskType.INGEST]
    test_config.state_fqn = STATE_FILE
    test_config.interval = 10
    if os.path.exists(test_config.progress_fqn):
        os.unlink(test_config.progress_fqn)

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_state(config=test_config,
                                  chooser=test_chooser,
                                  command_name=TEST_COMMAND,
                                  bookmark_name=TEST_BOOKMARK,
                                  end_time=test_end_time)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert fits2caom2_mock.called, 'expect fits2caom2 call'
    fits2caom2_mock.assert_called_once_with()

    test_state = mc.State(STATE_FILE)
    assert test_state.get_bookmark(TEST_BOOKMARK) == test_end_time, 'wrong time'
    assert os.path.exists(test_config.progress_fqn), 'expect progress file'

    # test that runner does nothing when times haven't changed
    start_time = test_end_time
    _write_state(start_time)
    fits2caom2_mock.reset_mock()
    test_result = rc.run_by_state(config=test_config,
                                  chooser=test_chooser,
                                  command_name=TEST_COMMAND,
                                  bookmark_name=TEST_BOOKMARK,
                                  end_time=test_end_time)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert not fits2caom2_mock.called, 'expect no fits2caom2 call'


@patch('caom2pipe.execute_composable.OrganizeExecutesWithDoOne.do_one')
def test_run_todo_list_dir_data_source_exception(do_one_mock, test_config):
    test_config.working_directory = TEST_DIR
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.log_to_file = True

    do_one_mock.side_effect = mc.CadcException

    if os.path.exists(test_config.failure_fqn):
        os.unlink(test_config.failure_fqn)
    if os.path.exists(test_config.retry_fqn):
        os.unlink(test_config.retry_fqn)

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config,
                                 chooser=test_chooser,
                                 command_name=TEST_COMMAND)
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


def _write_state(start_time):
    if os.path.exists(STATE_FILE):
        os.unlink(STATE_FILE)
    test_bookmark = {'bookmarks': {TEST_BOOKMARK:
                                   {'last_record': start_time}}}
    mc.write_as_yaml(test_bookmark, STATE_FILE)


call_count = 0


def _mock_query(arg1, arg2, arg3):
    global call_count
    if call_count == 0:
        call_count = 1
        return Table.read(
            'fileName,ingestDate\n'
            'NEOS_SCI_2015347000000_clean.fits,'
            '2019-10-23T16:27:19.000\n'.split('\n'),
            format='csv')
    else:
        return Table.read(
            'fileName,ingestDate\n'.split('\n'),
            format='csv')


def _mock_write():
    fqn = f'{tc.THIS_DIR}/NEOS_SCI_2015347000000_clean/' \
          f'NEOS_SCI_2015347000000_clean.fits.xml'
    mc.write_obs_to_file(
        SimpleObservation(collection='test_collection', observation_id='ghi',
                          algorithm=Algorithm(str('test'))), fqn)

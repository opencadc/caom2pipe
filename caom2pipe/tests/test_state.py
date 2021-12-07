# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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
#  : 4 $
#
# ***********************************************************************
#

import glob
import logging
import os

from datetime import datetime, timedelta

import dateutil.tz
from unittest.mock import patch, Mock

from cadcdata import FileInfo
from caom2pipe import data_source_composable as dsc
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from caom2pipe import run_composable as rc
from caom2pipe import transfer_composable

import test_conf as tc


class TestTransfer(transfer_composable.Transfer):
    def __init__(self):
        super().__init__()

    def get(self, source_fqn, dest_fqn):
        logging.error(f'source {source_fqn} dest {dest_fqn}')

        test_source_fqn = '/caom2pipe_test/1000003f.fits.fz'
        test_source_uri = 'cadc:TEST/test_file.fits.gz'
        if source_fqn not in [test_source_fqn, test_source_uri]:
            assert False, f'wrong source directory {source_fqn}'
        assert (
            dest_fqn
            in [
                '/usr/src/app/caom2pipe/int_test/test_obs_id/test_file.fits.gz',
                '/usr/src/app/caom2pipe/int_test/test_obs_id/1000003f.fits.fz',
            ]
        ), 'wrong destination directory'
        with open(dest_fqn, 'w') as f:
            f.write('test content')


class TestListDirTimeBoxDataSource(dsc.DataSource):
    def __init__(self):
        super().__init__()

    def get_time_box_work(self, prev_exec_time, exec_time):
        result = []
        file_list = glob.glob('/caom2pipe_test/*')
        for entry in file_list:
            stats = os.stat(entry)
            if prev_exec_time <= stats.st_mtime <= exec_time:
                result.append(
                    dsc.StateRunnerMeta(entry, stats.st_mtime)
                )
        return result


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_state(client_mock):
    client_mock.metadata_client.read.side_effect = tc.mock_read
    client_mock.data_client.get_file.side_effect = tc.mock_get_file
    metadata_reader_mock = Mock(autospec=True)

    test_wd = '/usr/src/app/caom2pipe/int_test'
    caom2pipe_bookmark = 'caom2_timestamp'
    test_config = mc.Config()
    test_config.working_directory = test_wd
    test_config.collection = 'TEST'
    test_config.interval = 10
    test_config.log_file_directory = f'{test_wd}/logs'
    test_config.failure_fqn = (
        f'{test_config.log_file_directory}/failure_log.txt'
    )
    test_config.log_to_file = True
    test_config.logging_level = 'DEBUG'
    test_config.progress_file_name = 'progress.txt'
    test_config.proxy_file_name = f'{test_wd}/cadcproxy.pem'
    test_config.rejected_file_name = 'rejected.yml'
    test_config.rejected_directory = f'{test_wd}/rejected'
    test_config._report_fqn = (
        f'{test_config.log_file_directory}/app_report.txt'
    )
    test_config.resource_id = 'ivo://cadc.nrc.ca/sc2repo'
    test_config.retry_file_name = 'retries.txt'
    test_config.retry_fqn = (
        f'{test_config.log_file_directory}/{test_config.retry_file_name}'
    )
    test_config.state_file_name = 'state.yml'
    test_config.success_fqn = (
        f'{test_config.log_file_directory}/success_log.txt'
    )
    test_config.tap_id = 'ivo://cadc.nrc.ca/sc2tap'
    test_config.task_types = [
        mc.TaskType.STORE,
        mc.TaskType.INGEST,
        mc.TaskType.MODIFY,
    ]
    test_config.features.use_file_names = True
    test_config.features.use_urls = False
    test_config.features.supports_latest_client = False
    test_config.use_local_files = False
    test_config.storage_inventory_resource_id = 'ivo://cadc.nrc.ca/test'

    if not os.path.exists(test_wd):
        os.mkdir(test_wd)

    test_start_time, test_end_time = _get_times(
        test_config, caom2pipe_bookmark
    )

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content\n')

    test_data_source = TestListDirTimeBoxDataSource()
    test_builder = nbc.GuessingBuilder(tc.TestStorageName)
    transferrer = TestTransfer()

    try:
        test_result = rc.run_by_state(
            bookmark_name=caom2pipe_bookmark,
            config=test_config,
            end_time=test_end_time,
            name_builder=test_builder,
            source=test_data_source,
            modify_transfer=transferrer,
            store_transfer=transferrer,
            clients=client_mock,
            metadata_reader=metadata_reader_mock,
        )

        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'expect success'

        # state file checking
        test_state = mc.State(test_config.state_fqn)
        assert test_state is not None, 'expect state content'
        test_checkpoint = test_state.get_bookmark(caom2pipe_bookmark)
        assert test_checkpoint == test_end_time, 'wrong bookmark'

        # success file testing
        assert os.path.exists(test_config.log_file_directory), 'log directory'
        assert os.path.exists(test_config.success_fqn), 'success fqn'
        assert os.path.exists(test_config.progress_fqn), 'progress fqn'
        log_file = f'{test_config.log_file_directory}/test_obs_id.log'
        actual = glob.glob(f'{test_config.log_file_directory}/**')
        actual_str = '\n'.join(ii for ii in actual)
        assert os.path.exists(log_file), f'specific log file {actual_str}'
        xml_file = f'{test_config.log_file_directory}/test_obs_id.xml'
        assert os.path.exists(xml_file), f'xml file {actual}'

        # reporting testing
        report_file = f'{test_config.log_file_directory}/app_report.txt'
        assert os.path.exists(report_file), f'report file {actual}'
        pass_through_test = False
        with open(report_file) as f:
            for line in f:
                pass_through_test = True
                if 'Number' in line:
                    bits = line.split(':')
                    found = False
                    if 'Inputs' in bits[0]:
                        assert bits[1].strip() == '1', 'wrong inputs'
                        found = True
                    elif 'Successes' in bits[0]:
                        assert bits[1].strip() == '1', 'wrong successes'
                        found = True
                    elif 'Timeouts' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong timeouts'
                        found = True
                    elif 'Retries' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong retries'
                        found = True
                    elif 'Errors' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong errors'
                        found = True
                    elif 'Rejections' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong rejections'
                        found = True
                    assert found, f'{line}'
        assert pass_through_test, 'found a report file and checked it'
    finally:
        f_list = glob.glob(f'{test_wd}/**', recursive=True)
        for entry in f_list:
            try:
                if os.path.isdir(entry):
                    os.rmdir(entry)
                else:
                    os.unlink(entry)
            except OSError as e:
                logging.error(f'failed to delete {e}')


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_state_v(client_mock):
    client_mock.metadata_client.read.side_effect = tc.mock_read
    client_mock.data_client.info.return_value = FileInfo(
        id='cadc:TEST/anything.fits',
        size=42,
        md5sum='9473fdd0d880a43c21b7778d34872157',
    )
    metadata_reader_mock = Mock(autospec=True)
    test_wd = '/usr/src/app/caom2pipe/int_test'
    caom2pipe_bookmark = 'caom2_timestamp'
    test_config = mc.Config()
    test_config.working_directory = test_wd
    test_config.collection = 'TEST'
    test_config.interval = 10
    test_config.log_file_directory = f'{test_wd}/logs'
    test_config.failure_fqn = (
        f'{test_config.log_file_directory}/failure_log.txt'
    )
    test_config.log_to_file = True
    test_config.logging_level = 'INFO'
    test_config.progress_file_name = 'progress.txt'
    test_config.proxy_file_name = f'{test_wd}/cadcproxy.pem'
    test_config.rejected_file_name = 'rejected.yml'
    test_config.rejected_directory = f'{test_wd}/rejected'
    test_config._report_fqn = (
        f'{test_config.log_file_directory}/app_report.txt'
    )
    test_config.resource_id = 'ivo://cadc.nrc.ca/sc2repo'
    test_config.retry_file_name = 'retries.txt'
    test_config.retry_fqn = (
        f'{test_config.log_file_directory}/{test_config.retry_file_name}'
    )
    test_config.state_file_name = 'state.yml'
    test_config.success_fqn = (
        f'{test_config.log_file_directory}/success_log.txt'
    )
    test_config.tap_id = 'ivo://cadc.nrc.ca/sc2tap'
    test_config.task_types = [
        mc.TaskType.STORE,
        mc.TaskType.INGEST,
        mc.TaskType.MODIFY,
    ]
    test_config.features.use_file_names = True
    test_config.features.use_urls = False
    test_config.features.supports_latest_client = True
    test_config.use_local_files = False
    test_config.storage_inventory_resource_id = 'ivo://cadc.nrc.ca/test'

    if not os.path.exists(test_wd):
        os.mkdir(test_wd)

    test_start_time, test_end_time = _get_times(
        test_config, caom2pipe_bookmark
    )

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content\n')

    test_data_source = TestListDirTimeBoxDataSource()
    test_builder = nbc.GuessingBuilder(tc.TestStorageName)
    transferrer = TestTransfer()

    try:
        test_result = rc.run_by_state(
            bookmark_name=caom2pipe_bookmark,
            config=test_config,
            end_time=test_end_time,
            name_builder=test_builder,
            source=test_data_source,
            modify_transfer=transferrer,
            store_transfer=transferrer,
            clients=client_mock,
            metadata_reader=metadata_reader_mock,
        )

        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'expect success'
        assert client_mock.data_client.put.called, 'expect put call'
        client_mock.data_client.put.assert_called_with(
            '/usr/src/app/caom2pipe/int_test/test_obs_id',
            'cadc:TEST/test_file.fits.gz',
            None,
        ), 'wrong call args'

        # state file checking
        test_state = mc.State(test_config.state_fqn)
        assert test_state is not None, 'expect state content'
        test_checkpoint = test_state.get_bookmark(caom2pipe_bookmark)
        assert test_checkpoint == test_end_time, 'wrong bookmark'

        # success file testing
        assert os.path.exists(test_config.log_file_directory), 'log directory'
        assert os.path.exists(test_config.success_fqn), 'success fqn'
        assert os.path.exists(test_config.progress_fqn), 'progress fqn'
        log_file = f'{test_config.log_file_directory}/test_obs_id.log'
        actual = glob.glob(f'{test_config.log_file_directory}/**')
        assert os.path.exists(log_file), f'specific log file {actual}'
        xml_file = f'{test_config.log_file_directory}/test_obs_id.xml'
        assert os.path.exists(xml_file), f'xml file {actual}'

        # reporting testing
        report_file = f'{test_config.log_file_directory}/app_report.txt'
        assert os.path.exists(report_file), f'report file {actual}'
        pass_through_test = False
        with open(report_file) as f:
            for line in f:
                pass_through_test = True
                if 'Number' in line:
                    bits = line.split(':')
                    found = False
                    if 'Inputs' in bits[0]:
                        assert bits[1].strip() == '1', 'wrong inputs'
                        found = True
                    elif 'Successes' in bits[0]:
                        assert bits[1].strip() == '1', 'wrong successes'
                        found = True
                    elif 'Timeouts' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong timeouts'
                        found = True
                    elif 'Retries' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong retries'
                        found = True
                    elif 'Errors' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong errors'
                        found = True
                    elif 'Rejections' in bits[0]:
                        assert bits[1].strip() == '0', 'wrong rejections'
                        found = True
                    assert found, f'{line}'
        assert pass_through_test, 'found a report file and checked it'
    finally:
        f_list = glob.glob(f'{test_wd}/**', recursive=True)
        for entry in f_list:
            try:
                if os.path.isdir(entry):
                    os.rmdir(entry)
                else:
                    os.unlink(entry)
            except OSError as e:
                logging.error(f'failed to delete {e}')


def _get_times(test_config, caom2pipe_bookmark):
    if not os.path.exists('/caom2pipe_test'):
        os.mkdir('/caom2pipe_test')
        from pathlib import Path
        Path('/caom2pipe_test/1000003f.fits.fz').touch()

    test_start_time = datetime.fromtimestamp(
        os.stat('/caom2pipe_test/1000003f.fits.fz').st_mtime,
        tz=dateutil.tz.UTC
    ) - timedelta(minutes=5)

    with open(test_config.state_fqn, 'w') as f:
        f.write('bookmarks:\n')
        f.write(f'  {caom2pipe_bookmark}:\n')
        f.write(f'    last_record: {test_start_time}\n')
    test_end_time = test_start_time + timedelta(minutes=12)
    return test_start_time, test_end_time

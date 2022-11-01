# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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

import logging
import os
import pytest
import sys

from unittest.mock import Mock, patch, ANY

from astropy.io import fits
from hashlib import md5
from shutil import copy
from tempfile import TemporaryDirectory

from cadcdata import FileInfo
from caom2 import SimpleObservation, Algorithm

from caom2pipe.client_composable import ClientCollection
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe.reader_composable import FileMetadataReader
from caom2pipe import transfer_composable
import test_conf as tc

TEST_APP = 'collection2caom2'


class MyExitError(Exception):
    pass


class TestVisit:
    @staticmethod
    def visit(observation, **kwargs):
        x = kwargs['working_directory']
        assert x is not None, 'working directory'
        y = kwargs['storage_name']
        assert y is not None, 'storage_name'
        z = kwargs['log_file_directory']
        assert z is not None, 'log file directory'
        assert observation is not None, 'undefined observation'
        return observation


class LocalTestVisit:
    @staticmethod
    def visit(observation, **kwargs):
        x = kwargs['working_directory']
        assert x is not None, 'working directory'
        assert (
            x == f'{tc.TEST_DATA_DIR}/test_obs_id'
        ), 'wrong working directory'
        y = kwargs['storage_name']
        assert y is not None, 'storage name'
        assert (
            y.destination_uris[0] == 'cadc:TEST/test_file.fits'
        ), 'wrong science file'
        z = kwargs['log_file_directory']
        assert z is not None, 'log file directory'
        assert z == tc.TEST_DATA_DIR, 'wrong log dir'
        assert observation is not None, 'undefined observation'
        return observation


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_meta_visit_delete_create_execute(access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    repo_client_mock.read.side_effect = _read_obs2
    clients = ClientCollection(test_config)
    clients._data_client = data_client_mock
    clients._metadata_client = repo_client_mock
    test_observer = Mock()
    with TemporaryDirectory() as tmp_dir_name:
        test_sn = tc.TestStorageName()
        test_config.working_directory = tmp_dir_name
        os.mkdir(os.path.join(tmp_dir_name, test_sn.obs_id))
        test_executor = ec.MetaVisitDeleteCreate(
            test_config,
            [],
            observable=test_observer,
            metadata_reader=Mock(autospec=True),
            clients=clients,
        )
        test_executor.execute({'storage_name': test_sn})
        assert repo_client_mock.delete.called, 'delete call missed'
        assert repo_client_mock.create.called, 'create call missed'
        assert test_observer.metrics.observe.called, 'observe not called'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_client_visit(access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    data_client_mock = Mock()
    repo_client_mock = Mock()
    metadata_reader_mock = Mock()
    test_observer = Mock()
    clients = ClientCollection(test_config)
    clients._data_client = data_client_mock
    clients._metadata_client = repo_client_mock

    with patch('caom2pipe.manage_composable.write_obs_to_file') as write_mock:
        mc.StorageName.collection = 'TEST'
        test_executor = ec.MetaVisit(
            test_config,
            meta_visitors=None,
            observable=test_observer,
            metadata_reader=metadata_reader_mock,
            clients=clients,
        )

        test_executor.execute({'storage_name': tc.TestStorageName()})
        repo_client_mock.read.assert_called_with(
            'TEST', 'test_obs_id'
        ), 'read call missed'
        assert repo_client_mock.update.called, 'update call missed'
        assert test_observer.metrics.observe.called, 'observe not called'
        assert write_mock.called, 'write mock not called'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_data_execute(access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    mc.StorageName.collection = 'TEST'
    test_obs_id = 'test_obs_id'
    with TemporaryDirectory() as tmp_dir_name:
        test_dir = os.path.join(tmp_dir_name, test_obs_id)
        test_config.working_directory = tmp_dir_name
        os.mkdir(test_dir, mode=0o755)
        test_fits_fqn = os.path.join(test_dir, tc.TestStorageName().file_name)
        precondition = open(test_fits_fqn, 'w')
        precondition.close()

        test_data_visitors = [TestVisit]
        repo_client_mock = Mock()
        data_client_mock = Mock()
        clients = ClientCollection(test_config)
        clients._data_client = data_client_mock
        clients._metadata_client = repo_client_mock
        test_observer = Mock()
        test_transferrer = transfer_composable.VoTransfer()
        test_transferrer.cadc_client = data_client_mock

        ec.CaomExecute._data_cmd_info = Mock(side_effect=_get_fname)
        repo_client_mock.read.side_effect = tc.mock_read

        # run the test
        test_executor = ec.DataVisit(
            test_config,
            test_data_visitors,
            test_observer,
            test_transferrer,
            clients,
            metadata_reader=None,
        )
        test_executor.execute({'storage_name': tc.TestStorageName()})

        # check that things worked as expected
        assert repo_client_mock.read.called, 'read call missed'
        assert repo_client_mock.update.called, 'update call missed'
        assert test_observer.metrics.observe.called, 'observe not called'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_data_execute_v(access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    mc.StorageName.collection = 'TEST'
    test_config.features.supports_latest_client = True
    test_obs_id = 'test_obs_id'
    with TemporaryDirectory() as tmp_dir_name:
        test_config.working_directory = tmp_dir_name
        test_dir = os.path.join(tmp_dir_name, test_obs_id)
        test_fits_fqn = os.path.join(test_dir, 'test_obs_id.fits.gz')
        if not os.path.exists(test_dir):
            os.mkdir(test_dir, mode=0o755)

        test_data_visitors = [TestVisit]
        repo_client_mock = Mock(autospec=True)
        cadc_client_mock = Mock(autospec=True)
        cadc_client_mock.copy.side_effect = tc.mock_copy_md5
        clients = ClientCollection(test_config)
        clients._data_client = cadc_client_mock
        clients._metadata_client = repo_client_mock
        test_observer = Mock(autospec=True)
        test_transferrer = transfer_composable.VoTransfer()
        test_transferrer.cadc_client = cadc_client_mock

        ec.CaomExecute._data_cmd_info = Mock(side_effect=_get_fname)
        repo_client_mock.read.side_effect = tc.mock_read

        test_sn = tc.TestStorageName()
        test_sn.source_names = ['ad:TEST/test_obs_id.fits.gz']

        # run the test
        test_executor = ec.DataVisit(
            test_config,
            test_data_visitors,
            test_observer,
            test_transferrer,
            clients,
            metadata_reader=None,
        )
        test_executor.execute({'storage_name': test_sn})

        # check that things worked as expected
        assert repo_client_mock.read.called, 'read call missed'
        assert repo_client_mock.update.called, 'update call missed'
        assert test_observer.metrics.observe.called, 'observe not called'
        assert cadc_client_mock.copy.called, 'copy not called'
        cadc_client_mock.copy.assert_called_with(
            'cadc:TEST/test_obs_id.fits',
            test_fits_fqn.replace('.gz', ''),
            send_md5=True,
        ), 'wrong call args'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_data_local_execute(access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    # ensure the model uses the log directory for writing the model file
    mc.StorageName.collection = 'TEST'
    test_config.log_to_file = True
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.features.supports_latest_client = False
    test_data_visitors = [LocalTestVisit]

    data_client_mock = Mock()
    repo_client_mock = Mock()
    repo_client_mock.read.return_value = _read_obs(None)
    clients = ClientCollection(test_config)
    clients._data_client = data_client_mock
    clients._metadata_client = repo_client_mock
    test_observer = Mock()

    test_model_fqn = os.path.join(tc.TEST_DATA_DIR, 'test_obs_id.xml')
    # check that a file is written to disk
    if os.path.exists(test_model_fqn):
        os.unlink(test_model_fqn)

    # run the test
    test_executor = ec.LocalDataVisit(
        test_config,
        test_data_visitors,
        observable=test_observer,
        clients=clients,
        metadata_reader=None,
    )
    test_executor.execute({'storage_name': tc.TestStorageName(
        source_names=[f'{tc.TEST_DATA_DIR}/test_file.fits.gz'],
    )})

    # check that things worked as expected - no cleanup
    assert repo_client_mock.read.called, 'read call missed'
    repo_client_mock.read.assert_called_with(
        'TEST', 'test_obs_id'
    ), 'wrong repo client read args'
    assert repo_client_mock.update.called, 'update call missed'
    assert test_observer.metrics.observe.called, 'observe not called'
    assert os.path.exists(test_model_fqn), 'observation not written to disk'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.FitsForCADCDecompressor.fix_compression')
def test_data_store(fix_mock, access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    test_dir = f'{tc.TEST_DATA_DIR}/test_obs_id'
    if os.path.exists(test_dir):
        os.rmdir(test_dir)

    data_client_mock = Mock()
    clients = ClientCollection(test_config)
    clients._data_client = data_client_mock
    test_observer = Mock()
    # stat mock is for CadcDataClient
    stat_orig = os.stat
    os.stat = Mock()
    os.stat.st_size.return_value = 1243
    # path.exists mock is because the stat mock causes os.path.exists to
    # return True
    path_orig = os.path.exists
    os.path.exists = Mock(return_value=False)
    test_config.features.supports_multiple_files = False
    try:
        test_config.working_directory = tc.TEST_DATA_DIR
        test_executor = ec.Store(
            test_config,
            observable=test_observer,
            transferrer=transfer_composable.Transfer(),
            clients=clients,
            metadata_reader=Mock(),
        )
        test_executor.execute({'storage_name': tc.TestStorageName(
            source_names=[f'{tc.TEST_DATA_DIR}/test_file.fits.gz'],
        )})

        # check that things worked as expected - no cleanup
        assert data_client_mock.put.called, 'put_file call missed'
    finally:
        os.stat = stat_orig
        os.path.exists = path_orig


def test_scrape(test_config):
    # clean up from previous tests
    test_sn = tc.TestStorageName()
    model_fqn = os.path.join(tc.TEST_DATA_DIR, test_sn.model_file_name)
    if os.path.exists(model_fqn):
        os.remove(model_fqn)

    with TemporaryDirectory() as tmp_dir_name:
        test_config.working_directory = tmp_dir_name
        test_config.logging_level = 'INFO'
        test_reader = FileMetadataReader()
        test_sn = tc.TestStorageName(source_names=[f'{tc.TEST_FILES_DIR}/correct.fits'])
        test_executor = ec.Scrape(
            test_config,
            observable=None,
            meta_visitors=[],
            metadata_reader=test_reader,
        )
        os.mkdir(os.path.join(tmp_dir_name, test_sn.obs_id))
        test_executor.execute({'storage_name': test_sn})


def test_data_scrape_execute(test_config):
    test_config.log_to_file = True
    test_data_visitors = [TestVisit]
    with TemporaryDirectory() as tmp_dir_name:
        test_config.working_directory = tmp_dir_name
        test_sn = tc.TestStorageName()
        # run the test
        test_executor = ec.DataScrape(
            test_config,
            test_data_visitors,
            observable=None,
            metadata_reader=None,
        )
        os.mkdir(os.path.join(tmp_dir_name, test_sn.obs_id))
        copy(
            f'{tc.TEST_DATA_DIR}/fpf_start_obs.xml',
            f'{tc.TEST_DATA_DIR}/test_obs_id.xml',
        )
        test_executor.execute({'storage_name': test_sn})


def test_organize_executes_chooser(test_config):
    test_config.use_local_files = True
    test_config.features.supports_latest_client = False
    log_file_directory = os.path.join(tc.THIS_DIR, 'logs')
    test_config.log_file_directory = log_file_directory
    test_config.features.supports_composite = True

    test_config.task_types = [mc.TaskType.INGEST]
    test_chooser = tc.TestChooser()
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        test_chooser,
        clients=Mock(autospec=True, return_value=None),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 1
    assert isinstance(test_oe._executors[0], ec.MetaVisitDeleteCreate)
    test_oe._executors[0].storage_name = tc.TestStorageName()
    assert test_oe._executors[0].working_dir == os.path.join(tc.THIS_DIR, 'test_obs_id'), 'working_dir'
    test_config.use_local_files = False
    test_config.task_types = [mc.TaskType.INGEST]
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        test_chooser,
        clients=Mock(autospec=True, return_value=None),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 1
    assert isinstance(test_oe._executors[0], ec.MetaVisitDeleteCreate)


def test_organize_executes_client_existing(test_config):
    test_config.features.use_clients = True
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.use_local_files = False
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        metadata_reader=Mock(autospec=True),
        clients=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 1
    assert isinstance(test_oe._executors[0], ec.MetaVisit)


def test_organize_executes_client_visit(test_config):
    test_config.features.use_clients = True
    test_config.task_types = [mc.TaskType.VISIT]
    test_config.use_local_files = False
    clients_mock = Mock(autospec=True)
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        metadata_reader=Mock(autospec=True),
        clients=clients_mock,
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 1
    assert isinstance(test_oe._executors[0], ec.MetaVisit)
    clients_mock.return_value.metadata_client.read.assert_not_called, 'mock should not be called?'


def test_do_one(test_config):
    test_config.task_types = []
    test_reader = Mock(autospec=True)
    test_organizer = ec.OrganizeExecutes(
        test_config, 'test2caom2', [], [], metadata_reader=test_reader
    )
    # no client
    test_result = test_organizer.do_one(tc.TestStorageName())
    assert test_result is not None
    assert test_result == -1

    # client
    test_config.features.use_clients = True
    test_result = test_organizer.do_one(tc.TestStorageName())
    assert test_result is not None
    assert test_result == -1


def test_storage_name():
    mc.StorageName.collection = 'TEST'
    mc.StorageName.collection_pattern = 'T[\\w+-]+'
    sn = mc.StorageName(obs_id='test_obs_id', file_name='test_obs_id.fits.gz')
    assert sn.file_uri == 'cadc:TEST/test_obs_id.fits'
    assert sn.file_name == 'test_obs_id.fits.gz'
    assert sn.model_file_name == 'test_obs_id.xml'
    assert sn.prev == 'test_obs_id_prev.jpg'
    assert sn.thumb == 'test_obs_id_prev_256.jpg'
    assert sn.prev_uri == 'cadc:TEST/test_obs_id_prev.jpg'
    assert sn.thumb_uri == 'cadc:TEST/test_obs_id_prev_256.jpg'
    assert sn.obs_id == 'test_obs_id'
    assert sn.log_file == 'test_obs_id.log'
    assert sn.product_id == 'test_obs_id'
    assert not sn.is_valid()
    sn = mc.StorageName(file_name='Test_obs_id.fits.gz')
    assert sn.is_valid()
    x = mc.StorageName.remove_extensions('test_obs_id.fits.header.gz')
    assert x == 'test_obs_id'


def test_caom_name():
    cn = mc.CaomName(uri='ad:TEST/test_obs_id.fits.gz')
    assert cn.file_id == 'test_obs_id'
    assert cn.file_name == 'test_obs_id.fits.gz'
    assert cn.uncomp_file_name == 'test_obs_id.fits'
    assert (
        mc.CaomName.make_obs_uri_from_obs_id('TEST', 'test_obs_id')
        == 'caom:TEST/test_obs_id'
    )


def test_omm_name_dots():
    TEST_NAME = 'C121121_J024345.57-021326.4_K_SCIRED'
    TEST_URI = f'ad:OMM/{TEST_NAME}.fits.gz'
    test_file_id = mc.CaomName(TEST_URI).file_id
    assert TEST_NAME == test_file_id, 'dots messing with things'


@patch('sys.exit', Mock(side_effect=MyExitError))
def test_choose_exceptions(test_config):
    test_config.init_local_files = False
    test_config.task_types = [mc.TaskType.SCRAPE]
    with pytest.raises(mc.CadcException):
        test_organizer = ec.OrganizeExecutes(
            test_config, 'command name', [], []
        )
        test_organizer.choose()


@patch('sys.exit', Mock(side_effect=MyExitError))
def test_storage_name_failure(test_config):
    test_config.log_to_file = True
    assert not os.path.exists(test_config.success_fqn)
    assert not os.path.exists(test_config.failure_fqn)
    assert not os.path.exists(test_config.retry_fqn)
    test_organizer = ec.OrganizeExecutes(test_config, 'command name', [], [])
    test_organizer.choose()
    assert os.path.exists(test_config.success_fqn)
    assert os.path.exists(test_config.failure_fqn)
    assert os.path.exists(test_config.retry_fqn)


def test_organize_executes_client_do_one(test_config):
    test_config.use_local_files = True
    log_file_directory = os.path.join(tc.THIS_DIR, 'logs')
    test_config.log_file_directory = log_file_directory
    success_log_file_name = 'success_log.txt'
    test_config.success_log_file_name = success_log_file_name
    failure_log_file_name = 'failure_log.txt'
    test_config.failure_log_file_name = failure_log_file_name
    test_config.features.use_clients = True
    retry_file_name = 'retries.txt'
    test_config.retry_file_name = retry_file_name
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        chooser=None,
        clients=Mock(autospec=True),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 1
    assert isinstance(test_oe._executors[0], ec.Scrape), f'{type(test_oe._executors[0])}'

    test_config.task_types = [
        mc.TaskType.STORE,
        mc.TaskType.INGEST,
        mc.TaskType.MODIFY,
    ]
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        chooser=None,
        clients=Mock(autospec=True),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 3
    assert (isinstance(test_oe._executors[0], ec.Store), type(test_oe._executors[0]))
    assert isinstance(test_oe._executors[1], ec.MetaVisit)
    assert isinstance(test_oe._executors[2], ec.LocalDataVisit)

    test_config.use_local_files = False
    test_config.task_types = [mc.TaskType.INGEST, mc.TaskType.MODIFY]
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        chooser=None,
        clients=Mock(autospec=True),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 2
    assert isinstance(test_oe._executors[0], ec.MetaVisit)
    assert isinstance(test_oe._executors[1], ec.DataVisit)

    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.INGEST, mc.TaskType.MODIFY]
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        chooser=None,
        clients=Mock(autospec=True),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 2
    assert isinstance(test_oe._executors[0], ec.MetaVisit)
    assert isinstance(test_oe._executors[1], ec.LocalDataVisit)

    test_config.task_types = [mc.TaskType.SCRAPE, mc.TaskType.MODIFY]
    test_config.use_local_files = True
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        chooser=None,
        clients=Mock(autospec=True),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 2
    assert isinstance(test_oe._executors[0], ec.Scrape)
    assert isinstance(test_oe._executors[1], ec.DataScrape)

    test_config.task_types = [mc.TaskType.INGEST]
    test_config.use_local_files = False
    test_chooser = tc.TestChooser()
    test_oe = ec.OrganizeExecutes(
        test_config,
        [],
        [],
        chooser=test_chooser,
        clients=Mock(autospec=True),
        metadata_reader=Mock(autospec=True),
    )
    test_oe.choose()
    assert test_oe._executors is not None
    assert len(test_oe._executors) == 1
    assert isinstance(test_oe._executors[0], ec.MetaVisitDeleteCreate)


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2utils.data_util.StorageClientWrapper')
def test_data_visit(client_mock, access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    test_config.features.supports_latest_client = False
    client_mock.get.side_effect = Mock(autospec=True)
    test_repo_client = Mock(autospec=True)
    test_repo_client.read.side_effect = tc.mock_read
    clients = ClientCollection(test_config)
    clients._data_client = client_mock
    clients._metadata_client = test_repo_client
    dv_mock = Mock(autospec=True)
    dv_mock.visit.return_value = SimpleObservation(
        collection='test_collection',
        observation_id='test_obs_id',
        algorithm=Algorithm('exposure'),
    )
    test_data_visitors = [dv_mock]
    test_observable = Mock(autospec=True)
    mc.StorageName.collection = 'TEST'
    mc.StorageName.collection_pattern = 'T[\\w+-]+'
    test_sn = mc.StorageName(
        obs_id='test_obs_id',
        file_name='test_obs_id.fits',
        source_names=['ad:TEST/test_obs_id.fits'],
    )
    test_transferrer = transfer_composable.CadcTransfer()
    test_transferrer.cadc_client = client_mock
    test_transferrer.observable = test_observable

    with TemporaryDirectory() as tmp_dir_name:
        test_config.working_directory = tmp_dir_name
        test_subject = ec.DataVisit(
            test_config,
            test_data_visitors,
            test_observable,
            test_transferrer,
            clients,
            metadata_reader=None,
        )
        os.mkdir(os.path.join(tmp_dir_name, test_sn.obs_id))
        test_subject.execute({'storage_name': test_sn})
        assert client_mock.get.called, 'should be called'
        client_mock.get.assert_called_with(
            f'{tmp_dir_name}/test_obs_id', test_sn.destination_uris[0]
        ), 'wrong get call args'
        test_repo_client.read.assert_called_with(
            'TEST', 'test_obs_id'
        ), 'wrong values'
        assert test_repo_client.update.called, 'expect an execution'
        # TODO - why is the log file directory NOT the working directory?

        args, kwargs = dv_mock.visit.call_args
        assert kwargs.get('working_directory') == f'{tmp_dir_name}/test_obs_id'
        assert (
            kwargs.get('storage_name') == test_sn
        ), 'wrong storage name parameter'
        assert kwargs.get('log_file_directory') == tc.TEST_DATA_DIR


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.FitsForCADCDecompressor.fix_compression')
def test_store(compressor_mock, access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.features.supports_latest_client = False
    test_sn = tc.TestStorageName(
        obs_id='test_obs_id',
        source_names=['vos:goliaths/nonexistent.fits.gz'],
    )
    test_data_client = Mock(autospec=True)
    clients = ClientCollection(test_config)
    clients._data_client = test_data_client
    test_observable = Mock(autospec=True)
    test_transferrer = Mock(autospec=True)
    test_transferrer.get.side_effect = _transfer_get_mock
    compressor_mock.return_value = (
        f'{test_config.working_directory}/test_obs_id/test_file.fits'
    )
    with TemporaryDirectory() as tmp_dir_name:
        test_config.working_directory = tmp_dir_name
        test_subject = ec.Store(
            test_config,
            test_observable,
            test_transferrer,
            clients,
            metadata_reader=Mock(),
        )
        assert test_subject is not None, 'expect construction'
        os.mkdir(os.path.join(tmp_dir_name, test_sn.obs_id))
        test_subject.execute({'storage_name': test_sn})
        assert test_subject.working_dir == os.path.join(tmp_dir_name, 'test_obs_id'), 'wrong working directory'
        assert (
            len(test_subject._storage_name.destination_uris) == 1
        ), 'wrong file count'
        assert (
            test_subject._storage_name.destination_uris[0]
            == 'cadc:TEST/test_file.fits'
        ), 'wrong destination'
        assert test_data_client.put.called, 'data put not called'
        test_data_client.put.assert_called_with(
            f'{tmp_dir_name}/test_obs_id', 'cadc:TEST/test_file.fits', None
        ), 'wrong put call args'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.FitsForCADCDecompressor.fix_compression')
def test_local_store(compressor_mock, access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.data_source = ['/test_files/caom2pipe']
    test_config.use_local_files = True
    test_config.store_newer_files_only = False
    test_config.features.supports_latest_client = False
    test_sn = tc.TestStorageName(
        source_names=[f'{tc.TEST_DATA_DIR}/test_file.fits.gz'],
    )
    compressor_mock.return_value = test_sn.source_names[0].replace('.gz', '')

    if not os.path.exists(test_sn.source_names[0]):
        with open(test_sn.source_names[0], 'w') as f:
            f.write('test content')

    test_data_client = Mock(autospec=True)
    clients = ClientCollection(test_config)
    clients._data_client = test_data_client
    test_observable = Mock(autospec=True)
    test_subject = ec.LocalStore(
        test_config,
        # test_sn,
        test_observable,
        clients,
        metadata_reader=Mock(),
    )
    assert test_subject is not None, 'expect construction'
    test_subject.execute({'storage_name': test_sn})
    # does the working directory get used if it's just a local store?
    assert test_subject.working_dir == os.path.join(
        tc.TEST_DATA_DIR, 'test_obs_id'
    ), 'wrong working directory'
    # do one file at a time, so it doesn't matter how many files are
    # in the working directory
    assert (
        len(test_subject._storage_name.destination_uris) == 1
    ), 'wrong file count'
    assert (
        test_subject._storage_name.destination_uris[0]
        == 'cadc:TEST/test_file.fits'
    ), 'wrong destination'
    assert test_data_client.put.called, 'data put not called'
    assert (
        test_data_client.put.call_args.args[0] == tc.TEST_DATA_DIR
    ), 'working directory'
    assert (
        test_data_client.put.call_args.args[1] == test_sn.destination_uris[0]
    ), 'expect a file name'


class FlagStorageName(mc.StorageName):
    def __init__(self, file_name, source_names):
        super().__init__(
            file_name=file_name,
            obs_id='1000003f',
            source_names=source_names,
        )

    @property
    def file_name(self):
        return self._file_name


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2utils.data_util.StorageClientWrapper')
def test_store_newer_files_only_flag(client_mock, access_mock, test_config):
    access_mock.return_value = 'https://localhost:2022'
    # first test case
    # flag set to True, file is older at CADC, supports_latest_client = False
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.features.supports_latest_client = False
    test_f_name = '1000003f.fits.fz'
    sn = [os.path.join('/caom2pipe_test', test_f_name)]
    du = [f'cadc:TEST/{test_f_name}']
    test_sn = FlagStorageName(test_f_name, sn)
    observable_mock = Mock(autospec=True)
    client_mock.info.return_value = FileInfo(
        id=du[0], lastmod='Mon, 4 Mar 2019 19:05:41 GMT'
    )
    clients = ClientCollection(test_config)
    clients._data_client = client_mock

    test_subject = ec.LocalStore(
        test_config,
        # test_sn,
        observable_mock,
        clients,
        metadata_reader=Mock(),
    )
    test_subject.execute({'storage_name': test_sn})
    assert client_mock.put.called, 'expect put call'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2utils.data_util.StorageClientWrapper')
def test_store_newer_files_only_flag_client(
    client_mock, access_mock, test_config
):
    access_mock.return_value = 'https://localhost:2022'
    # just like the previous test, except supports_latest_client = True
    # first test case
    # flag set to True, file is older at CADC, supports_latest_client = False
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.features.supports_latest_client = True
    test_f_name = '1000003f.fits.fz'
    sn = [os.path.join('/caom2pipe_test', test_f_name)]
    du = [f'cadc:TEST/{test_f_name}']
    test_sn = FlagStorageName(test_f_name, sn)
    observable_mock = Mock(autospec=True)
    client_mock.cadcinfo.return_value = FileInfo(
        id=du[0], md5sum='d41d8cd98f00b204e9800998ecf8427e'
    )
    clients = ClientCollection(test_config)
    clients._data_client = client_mock

    test_subject = ec.LocalStore(
        test_config,
        # test_sn,
        observable_mock,
        clients,
        metadata_reader=Mock(),
    )
    test_subject.execute({'storage_name': test_sn})
    assert client_mock.put.called, 'expect copy call'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_data_visit_params(access_mock):
    access_mock.return_value = 'https://localhost:2022'
    test_wd = '/tmp/abc'
    if os.path.exists(test_wd):
        if os.path.isdir(test_wd):
            os.rmdir(test_wd)
        else:
            os.unlink(test_wd)
    storage_name = mc.StorageName(
        obs_id='abc',
        file_name='abc.fits.gz',
        source_names=['vos:DAO/incoming/abc.fits.gz'],
    )

    test_config = mc.Config()
    test_config.task_types = [mc.TaskType.MODIFY]
    test_config.working_directory = '/tmp'
    test_config.logging_level = 'DEBUG'
    test_config.proxy_file_name = 'proxy.pem'
    test_config.proxy_fqn = f'{tc.TEST_DATA_DIR}/proxy.pem'
    test_config.resource_id = 'ivo:cadc.nrc.ca/test'

    test_cadc_client = Mock(autospec=True)
    test_caom_client = Mock(autospec=True)
    test_caom_client.read.side_effect = _read_obs2
    clients = ClientCollection(test_config)
    clients._data_client = test_cadc_client
    clients._metadata_client = test_caom_client
    data_visitor = Mock(autospec=True)
    data_visitor.visit.return_value = SimpleObservation(
        collection='test_collection',
        observation_id='test_obs_id',
        algorithm=Algorithm('exposure'),
    )
    test_data_visitors = [data_visitor]
    test_observable = Mock(autospec=True)
    test_transferrer = Mock(autospec=True)

    with TemporaryDirectory() as tmp_dir_name:
        test_config.working_directory = tmp_dir_name
        os.mkdir(os.path.join(test_config.working_directory, storage_name.obs_id))
        test_config.use_local_files = False
        test_subject = ec.DataVisit(
            test_config,
            test_data_visitors,
            test_observable,
            test_transferrer,
            clients,
            metadata_reader=None,
        )
        assert test_subject is not None, 'broken ctor'
        test_subject.execute({'storage_name': storage_name})
        assert data_visitor.visit.called, 'expect visit call'
        data_visitor.visit.assert_called_with(
            ANY,
            working_directory=f'{tmp_dir_name}/abc',
            storage_name=storage_name,
            log_file_directory=None,
            observable=ANY,
            clients=ANY,
            metadata_reader=ANY,
        ), f'wrong visit params {storage_name.source_names}'
        data_visitor.visit.reset_mock()


def test_decompress():
    if os.path.exists('/tmp/abc.fits.fz'):
        os.unlink('/tmp/abc.fits.fz')

    class RecompressStorageName(mc.StorageName):
        def __init__(self, file_name):
            super().__init__(file_name=file_name, source_names=[file_name])

        @property
        def file_uri(self):
            # because this is the condition which causes recompression
            return f'{super().file_uri}.fz'

    test_files = [
        '/tmp/abc.tar.gz',
        '/tmp/def.csv',
        '/test_files/compression/abc.fits.gz',
        # '/test_files/compression/ghi.fits.bz2',
    ]

    for fqn in test_files:
        test_sn = RecompressStorageName(file_name=fqn)
        for test_subject in [
            ec.FitsForCADCDecompressor('/tmp', logging.DEBUG),
            ec.FitsForCADCCompressor('/tmp', logging.DEBUG, test_sn),
        ]:
            assert test_subject is not None, 'ctor failure'
            test_result = test_subject.fix_compression(fqn)

            if '.fits' in fqn:
                assert os.path.exists(test_result), f'expect {test_result}'
                if isinstance(test_subject, ec.FitsForCADCCompressor):
                    temp = os.path.basename(fqn).replace('.gz', '.fz')
                    fz_fqn = os.path.join('/tmp', temp)
                    assert os.path.exists(fz_fqn), f'expect {fz_fqn}'
                os.unlink(test_result)


def _transfer_get_mock(entry, fqn):
    assert entry == 'vos:goliaths/nonexistent.fits.gz', 'wrong entry'
    with open(fqn, 'w') as f:
        f.write('test content')


def _communicate():
    return ['return status', None]


def _get_headers(uri):
    x = """SIMPLE  =                    T / Written by IDL:  Fri Oct  6 01:48:35 2017
BITPIX  =                   32 / Bits per pixel
NAXIS   =                    2 / Number of dimensions
NAXIS1  =                 2048 /
NAXIS2  =                 2048 /
DATATYPE= 'REDUC   '           /Data type, SCIENCE/CALIB/REJECT/FOCUS/TEST
END
"""
    delim = '\nEND'
    extensions = [e + delim for e in x.split(delim) if e.strip()]
    headers = [fits.Header.fromstring(e, sep='\n') for e in extensions]
    return headers


def _get_test_metadata(subject, path):
    return {
        'size': 37,
        'md5sum': 'e330482de75d5c4c88ce6f6ef99035ea',
        'type': 'applicaton/octect-stream',
    }


def _get_test_file_meta(path):
    return _get_test_metadata(None, None)


def _read_obs(arg1):
    return SimpleObservation(
        collection='test_collection',
        observation_id='test_obs_id',
        algorithm=Algorithm('exposure'),
    )


def _read_obs2(arg1, arg2):
    return _read_obs(None)


def _get_fname():
    return 'TBD'


def _test_map_todo():
    """For a mock."""
    return ''


def _get_file_info():
    return {'fname': 'test_file.fits'}


def to_caom2_with_client(ignore):
    assert sys.argv is not None, 'expect sys.argv to be set'
    local_meta_create_answer = [
        'test_execute_composable',
        '--verbose',
        '--observation',
        'OMM',
        'test_obs_id',
        '--local',
        f'{tc.TEST_DATA_DIR}/test_file.fits.gz',
        '--out',
        f'{tc.THIS_DIR}/test_obs_id/test_obs_id.xml',
        '--lineage',
        'test_obs_id/cadc:TEST/test_obs_id.fits.gz',
    ]
    scrape_answer = [
        'test_execute_composable',
        '--verbose',
        '--observation',
        'OMM',
        'test_obs_id',
        '--local',
        f'{tc.TEST_DATA_DIR}/test_file.fits.gz',
        '--out',
        f'{tc.TEST_DATA_DIR}/test_obs_id/test_obs_id.xml',
        '--lineage',
        'test_obs_id/cadc:TEST/test_obs_id.fits.gz',
    ]
    # TaskType.SCRAPE (Scrape)
    if sys.argv != scrape_answer:
        # TaskType.INGEST (LocalMetaCreate)
        if sys.argv != local_meta_create_answer:
            assert False, (
                f'wrong sys.argv values \n{sys.argv} '
                f'\n{local_meta_create_answer}'
            )
    fqn_index = sys.argv.index('--out') + 1
    fqn = sys.argv[fqn_index]
    mc.write_obs_to_file(
        SimpleObservation(
            collection='test_collection',
            observation_id='test_obs_id',
            algorithm=Algorithm('exposure'),
        ),
        fqn,
    )


def _mock_get_file_info(file_id):
    return FileInfo(
        id=file_id,
        size=10290,
        md5sum='{}'.format(md5(b'-37').hexdigest()),
        file_type='image/jpeg',
    )

# -*- coding: utf-8 -*-
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

from unittest.mock import Mock, patch

from astropy.io import fits
from datetime import datetime
from hashlib import md5

from caom2 import SimpleObservation, Algorithm

from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
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
        y = kwargs['science_file']
        assert y is not None, 'science file'
        z = kwargs['log_file_directory']
        assert z is not None, 'log file directory'
        assert observation is not None, 'undefined observation'


class LocalTestVisit:
    @staticmethod
    def visit(observation, **kwargs):
        v = kwargs['stream']
        assert v is not None, 'stream'
        assert v == 'TEST', 'wrong stream'
        x = kwargs['working_directory']
        assert x is not None, 'working directory'
        assert (
            x == os.path.join(tc.TEST_DATA_DIR, 'test_obs_id')
        ), 'wrong working directory'
        y = kwargs['science_file']
        assert y is not None, 'science file'
        assert y == 'test_obs_id.fits', 'wrong science file'
        z = kwargs['log_file_directory']
        assert z is not None, 'log file directory'
        assert z == tc.TEST_DATA_DIR, 'wrong log dir'
        assert observation is not None, 'undefined observation'


def test_meta_create_client_execute(test_config):
    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    read_obs_orig = mc.read_obs_from_file
    mc.read_obs_from_file = Mock()
    mc.read_obs_from_file.return_value = _read_obs(None)

    test_executor = ec.MetaCreate(
        test_config,
        tc.TestStorageName(),
        TEST_APP,
        test_cred,
        data_client_mock,
        repo_client_mock,
        meta_visitors=None,
        observable=test_observer,
    )
    try:
        test_executor.execute(None)
        assert repo_client_mock.create.called, 'create call missed'
        assert test_executor.url == 'https://test_url/', 'url'
        assert test_observer.metrics.observe.called, 'observe not called'
    finally:
        mc.read_obs_from_file = read_obs_orig


@patch('caom2utils.fits2caom2.CadcDataClient')
@patch('caom2utils.fits2caom2.get_cadc_headers')
def test_meta_create_client_execute_failed_update(
        headers_mock, f2c2_data_client_mock, test_config
):
    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    read_obs_orig = mc.read_obs_from_file
    mc.read_obs_from_file = Mock()
    mc.read_obs_from_file.return_value = _read_obs(None)
    f2c2_data_client_mock.return_value.get_file_info.side_effect = \
        _mock_get_file_info
    headers_mock.side_effect = _get_headers

    test_executor = ec.MetaCreate(
        test_config,
        tc.TestStorageName(),
        'failedUpdateCollection2caom2',
        test_cred,
        data_client_mock,
        repo_client_mock,
        meta_visitors=None,
        observable=test_observer,
    )
    try:
        with pytest.raises(mc.CadcException):
            test_executor.execute(None)
        assert not repo_client_mock.create.called, 'should have no create call'
        assert test_executor.url == 'https://test_url/', 'url'
    finally:
        mc.read_obs_from_file = read_obs_orig


def test_meta_update_client_execute(test_config):
    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    test_executor = ec.MetaUpdate(
        test_config,
        tc.TestStorageName(),
        TEST_APP,
        test_cred,
        data_client_mock,
        repo_client_mock,
        _read_obs(None),
        meta_visitors=None,
        observable=test_observer,
    )
    test_executor.execute(None)
    assert repo_client_mock.update.called, 'update call missed'
    assert test_observer.metrics.observe.called, 'observer call missed'


def test_meta_delete_create_client_execute(test_config):
    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    test_executor = ec.MetaDeleteCreate(
        test_config,
        tc.TestStorageName(),
        TEST_APP,
        test_cred,
        data_client_mock,
        repo_client_mock,
        _read_obs(None),
        None,
        observable=test_observer,
    )
    test_executor.execute(None)
    assert repo_client_mock.delete.called, 'delete call missed'
    assert repo_client_mock.create.called, 'create call missed'
    assert test_observer.metrics.observe.called, 'observe not called'


def test_local_meta_create_client_execute(test_config):
    test_dir = os.path.join(tc.THIS_DIR, 'test_obs_id')
    test_f_fqn = os.path.join(test_dir, 'test_obs_id.fits.xml')
    if os.path.exists(test_f_fqn):
        os.unlink(test_f_fqn)
        os.rmdir(test_dir)

    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    test_config.logging_level = 'INFO'

    test_executor = ec.LocalMetaCreate(
        test_config,
        tc.TestStorageName(),
        __name__,
        test_cred,
        data_client_mock,
        repo_client_mock,
        meta_visitors=None,
        observable=test_observer,
    )
    test_executor.execute(None)
    assert repo_client_mock.create.called, 'create call missed'
    assert test_observer.metrics.observe.called, 'observe not called'


def test_local_meta_update_client_execute(test_config):
    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    test_executor = ec.LocalMetaUpdate(
        test_config,
        tc.TestStorageName(),
        TEST_APP,
        test_cred,
        data_client_mock,
        repo_client_mock,
        _read_obs(None),
        meta_visitors=None,
        observable=test_observer,
    )
    test_executor.execute(None)
    assert repo_client_mock.update.called, 'update call missed'
    assert test_observer.metrics.observe.called, 'observe not called'


def test_local_meta_delete_create_client_execute(test_config):
    test_cred = ''
    data_client_mock = Mock()
    data_client_mock.get_file_info.return_value = {'name': 'test_file.fits'}
    repo_client_mock = Mock()
    test_observer = Mock()
    test_executor = ec.LocalMetaDeleteCreate(
        test_config,
        tc.TestStorageName(),
        TEST_APP,
        test_cred,
        data_client_mock,
        repo_client_mock,
        meta_visitors=None,
        observation=_read_obs(None),
        observable=test_observer,
    )
    test_executor.execute(None)
    assert repo_client_mock.delete.called, 'delete call missed'
    assert repo_client_mock.create.called, 'create call missed'
    assert test_observer.metrics.observe.called, 'observe not called'


def test_client_visit(test_config):
    test_cred = None
    data_client_mock = Mock()
    repo_client_mock = Mock()
    test_observer = Mock()

    with patch('caom2pipe.manage_composable.write_obs_to_file') as write_mock:
        test_executor = ec.MetaVisit(
            test_config,
            tc.TestStorageName(), test_cred,
            data_client_mock,
            repo_client_mock,
            meta_visitors=None,
            observable=test_observer,
        )

        test_executor.execute(None)
        repo_client_mock.read.assert_called_with(
            'OMM', 'test_obs_id'
        ), 'read call missed'
        assert repo_client_mock.update.called, 'update call missed'
        assert test_observer.metrics.observe.called, 'observe not called'
        assert write_mock.called, 'write mock not called'


def test_data_execute(test_config):
    test_obs_id = 'test_obs_id'
    test_dir = os.path.join(tc.THIS_DIR, test_obs_id)
    test_fits_fqn = os.path.join(
        test_dir, tc.TestStorageName().file_name
    )
    try:
        if not os.path.exists(test_dir):
            os.mkdir(test_dir, mode=0o755)
        precondition = open(test_fits_fqn, 'w')
        precondition.close()
        logging.error(test_fits_fqn)

        test_data_visitors = [TestVisit]
        repo_client_mock = Mock()
        data_client_mock = Mock()
        test_observer = Mock()
        test_transferrer = transfer_composable.VoTransfer()
        test_transferrer.cadc_client = data_client_mock

        ec.CaomExecute._data_cmd_info = Mock(side_effect=_get_fname)
        repo_client_mock.read.side_effect = tc.mock_read

        # run the test
        test_executor = ec.DataVisit(
            test_config,
            tc.TestStorageName(),
            data_client_mock,
            repo_client_mock,
            test_data_visitors,
            mc.TaskType.MODIFY,
            test_observer,
            test_transferrer,
        )
        test_executor.execute(None)

        # check that things worked as expected
        assert repo_client_mock.read.called, 'read call missed'
        assert repo_client_mock.update.called, 'update call missed'
        assert test_observer.metrics.observe.called, 'observe not called'
    finally:
        if os.path.exists(test_fits_fqn):
            os.unlink(test_fits_fqn)
        if os.path.exists(test_dir):
            os.rmdir(test_dir)


def test_data_execute_v(test_config):
    test_config.features.supports_latest_client = True
    test_obs_id = 'test_obs_id'
    test_dir = os.path.join(tc.THIS_DIR, test_obs_id)
    fqn = os.path.join(test_dir, tc.TestStorageName().file_name)
    test_fits_fqn = f'{fqn}.gz'
    try:
        if not os.path.exists(test_dir):
            os.mkdir(test_dir, mode=0o755)

        test_data_visitors = [TestVisit]
        repo_client_mock = Mock(autospec=True)
        cadc_client_mock = Mock(autospec=True)
        cadc_client_mock.copy.side_effect = tc.mock_copy_md5
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
            test_sn,
            cadc_client_mock,
            repo_client_mock,
            test_data_visitors,
            mc.TaskType.MODIFY,
            test_observer,
            test_transferrer,
        )
        test_executor.execute(None)

        # check that things worked as expected
        assert repo_client_mock.read.called, 'read call missed'
        assert repo_client_mock.update.called, 'update call missed'
        assert test_observer.metrics.observe.called, 'observe not called'
        assert cadc_client_mock.copy.called, 'copy not called'
        cadc_client_mock.copy.assert_called_with(
            'ad:TEST/test_obs_id.fits.gz', test_fits_fqn, send_md5=True
        ), 'wrong call args'
    finally:
        if os.path.exists(test_fits_fqn):
            os.unlink(test_fits_fqn)
        if os.path.exists(test_dir):
            os.rmdir(test_dir)


def test_data_local_execute(test_config):
    # ensure the model uses the log directory for writing the model file
    test_config.log_to_file = True
    test_config.working_directory = tc.TEST_DATA_DIR
    test_data_visitors = [LocalTestVisit]

    data_client_mock = Mock()
    repo_client_mock = Mock()
    repo_client_mock.read.return_value = _read_obs(None)
    test_observer = Mock()

    test_model_fqn = os.path.join(tc.TEST_DATA_DIR, 'test_obs_id.fits.xml')
    # check that a file is written to disk
    if os.path.exists(test_model_fqn):
        os.unlink(test_model_fqn)

    # run the test
    test_executor = ec.LocalDataVisit(
        test_config,
        tc.TestStorageName(),
        data_client_mock,
        repo_client_mock,
        test_data_visitors,
        observable=test_observer
    )
    test_executor.execute(None)

    # check that things worked as expected - no cleanup
    assert repo_client_mock.read.called, 'read call missed'
    repo_client_mock.read.assert_called_with(
        'OMM', 'test_obs_id'
    ), 'wrong repo client read args'
    assert repo_client_mock.update.called, 'update call missed'
    assert test_observer.metrics.observe.called, 'observe not called'
    assert os.path.exists(test_model_fqn), 'observation not written to disk'


def test_data_store(test_config):
    test_dir = f'{tc.TEST_DATA_DIR}/test_obs_id'
    if os.path.exists(test_dir):
        os.rmdir(test_dir)

    data_client_mock = Mock()
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
            tc.TestStorageName(),
            'command_name',
            data_client_mock,
            observable=test_observer,
            transferrer=transfer_composable.Transfer(),
        )
        test_executor.execute(None)

        # check that things worked as expected - no cleanup
        assert data_client_mock.put_file.called, 'put_file call missed'
    finally:
        os.stat = stat_orig
        os.path.exists = path_orig


def test_scrape(test_config):
    # clean up from previous tests
    test_sn = tc.TestStorageName()
    model_fqn = os.path.join(tc.TEST_DATA_DIR, test_sn.model_file_name)
    if os.path.exists(model_fqn):
        os.remove(model_fqn)

    netrc = os.path.join(tc.TEST_DATA_DIR, 'test_netrc')
    assert os.path.exists(netrc)
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.logging_level = 'INFO'
    test_executor = ec.Scrape(
        test_config,
        tc.TestStorageName(),
        __name__,
        observable=None,
        meta_visitors=[],
    )
    test_executor.execute(None)


def test_data_scrape_execute(test_config):
    test_data_visitors = [TestVisit]
    read_orig = mc.read_obs_from_file
    mc.read_obs_from_file = Mock(side_effect=_read_obs)
    try:

        # run the test
        test_executor = ec.DataScrape(
            test_config,
            tc.TestStorageName(),
            test_data_visitors,
            observable=None,
        )
        test_executor.execute(None)
        assert mc.read_obs_from_file.called, 'read obs call missed'

    finally:
        mc.read_obs_from_file = read_orig


def test_organize_executes_chooser(test_config):
    test_obs_id = tc.TestStorageName()
    test_config.use_local_files = True
    log_file_directory = os.path.join(tc.THIS_DIR, 'logs')
    test_config.log_file_directory = log_file_directory
    test_config.features.supports_composite = True
    exec_cmd_orig = mc.exec_cmd_info
    caom_client = Mock(autospec=True)
    caom_client.read.side_effect = _read_obs2

    try:
        ec.CaomExecute.repo_cmd_get_client = Mock(return_value=_read_obs(None))
        mc.exec_cmd_info = Mock(
            return_value='INFO:cadc-data:info\n'
                         'File C170324_0054_SCI_prev.jpg:\n'
                         '    archive: OMM\n'
                         '   encoding: None\n'
                         '    lastmod: Mon, 25 Jun 2018 16:52:07 GMT\n'
                         '     md5sum: f37d21c53055498d1b5cb7753e1c6d6f\n'
                         '       name: C120902_sh2-132_J_old_'
                         'SCIRED.fits.gz\n'
                         '       size: 754408\n'
                         '       type: image/jpeg\n'
                         '    umd5sum: 704b494a972eed30b18b817e243ced7d\n'
                         '      usize: 754408\n'.encode('utf-8')
        )

        test_config.task_types = [mc.TaskType.INGEST]
        test_chooser = tc.TestChooser()
        test_oe = ec.OrganizeExecutes(
            test_config, 'command_name', [], [], test_chooser
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 1
        assert isinstance(executors[0], ec.LocalMetaDeleteCreate)
        assert executors[0].fname == 'test_obs_id.fits', 'file name'
        assert executors[0].stream == 'TEST', 'stream'
        assert (
            executors[0].working_dir == os.path.join(
                tc.THIS_DIR, 'test_obs_id'
            )
        ), 'working_dir'

        test_config.use_local_files = False
        test_config.task_types = [mc.TaskType.INGEST]
        test_oe = ec.OrganizeExecutes(
            test_config, 'command_name', [], [], test_chooser
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 1
        assert isinstance(executors[0], ec.MetaDeleteCreate)
        assert CadcDataClient.__init__.called, 'mock not called'
        assert CAOM2RepoClient.__init__.called, 'mock not called'
    finally:
        mc.exec_cmd_orig = exec_cmd_orig


def test_organize_executes_client_existing(test_config):
    test_obs_id = tc.TestStorageName()
    test_config.features.use_clients = True
    repo_client_mock = Mock(autospec=True)
    repo_client_mock.read.side_effect = _read_obs2
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.use_local_files = False
    test_oe = ec.OrganizeExecutes(
        test_config,
        'command_name',
        [],
        [],
        cadc_client=Mock(autospec=True),
        caom_client=repo_client_mock,
    )
    executors = test_oe.choose(test_obs_id)
    assert executors is not None
    assert len(executors) == 1
    assert isinstance(executors[0], ec.MetaUpdate)
    assert repo_client_mock.read.called, 'mock not called'


def test_organize_executes_client_visit(test_config):
    test_obs_id = tc.TestStorageName()
    test_config.features.use_clients = True
    test_config.task_types = [mc.TaskType.VISIT]
    test_config.use_local_files = False
    repo_client_mock = Mock(autospec=True)
    repo_client_mock.read.side_effect = _read_obs2
    test_oe = ec.OrganizeExecutes(
        test_config,
        'command_name',
        [],
        [],
        cadc_client=Mock(autospec=True),
        caom_client=repo_client_mock,
    )
    executors = test_oe.choose(test_obs_id)
    assert executors is not None
    assert len(executors) == 1
    assert isinstance(executors[0], ec.MetaVisit)
    assert not repo_client_mock.read.called, 'mock should not be called?'


def test_do_one(test_config):
    test_config.task_types = []
    test_organizer = ec.OrganizeExecutes(test_config, 'test2caom2', [], [])
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
    sn = mc.StorageName(
        obs_id='test_obs_id', collection='TEST', collection_pattern='T[\\w+-]+'
    )
    assert sn.file_uri == 'ad:TEST/test_obs_id.fits.gz'
    assert sn.file_name == 'test_obs_id.fits'
    assert sn.compressed_file_name == 'test_obs_id.fits.gz'
    assert sn.model_file_name == 'test_obs_id.fits.xml'
    assert sn.prev == 'test_obs_id_prev.jpg'
    assert sn.thumb == 'test_obs_id_prev_256.jpg'
    assert sn.prev_uri == 'ad:TEST/test_obs_id_prev.jpg'
    assert sn.thumb_uri == 'ad:TEST/test_obs_id_prev_256.jpg'
    assert sn.obs_id == 'test_obs_id'
    assert sn.log_file == 'test_obs_id.log'
    assert sn.product_id == 'test_obs_id'
    assert sn.fname_on_disk is None
    assert not sn.is_valid()
    sn = mc.StorageName(
        obs_id='Test_obs_id', collection='TEST', collection_pattern='T[\\w+-]+'
    )
    assert sn.is_valid()
    x = mc.StorageName.remove_extensions('test_obs_id.fits.header.gz')
    assert x == 'test_obs_id'


def test_caom_name():
    cn = mc.CaomName(uri='ad:TEST/test_obs_id.fits.gz')
    assert cn.file_id == 'test_obs_id'
    assert cn.file_name == 'test_obs_id.fits.gz'
    assert cn.uncomp_file_name == 'test_obs_id.fits'
    assert (
        mc.CaomName.make_obs_uri_from_obs_id('TEST', 'test_obs_id') ==
        'caom:TEST/test_obs_id'
    )


def test_omm_name_dots():
    TEST_NAME = 'C121121_J024345.57-021326.4_K_SCIRED'
    TEST_URI = f'ad:OMM/{TEST_NAME}.fits.gz'
    test_file_id = mc.CaomName(TEST_URI).file_id
    assert TEST_NAME == test_file_id, 'dots messing with things'


def test_meta_update_observation_direct(test_config):
    test_cred_param = '--cert /usr/src/app/cadcproxy.pem'
    data_client_mock = Mock()
    repo_client_mock = Mock()
    test_sn = tc.TestStorageName()
    test_observation = mc.read_obs_from_file(
        f'{tc.TEST_DATA_DIR}/fpf_start_obs.xml'
    )
    try:
        test_observable = mc.Observable(
            mc.Rejected(test_config.rejected_fqn), mc.Metrics(test_config)
        )
        test_executor = ec.MetaUpdateObservation(
            test_config,
            test_sn,
            TEST_APP,
            test_cred_param,
            data_client_mock,
            repo_client_mock,
            test_observation,
            [],
            observable=test_observable,
        )
        test_executor.execute(None)
        assert repo_client_mock.update.called, 'repo update called'
    finally:
        pass


@patch('sys.exit', Mock(side_effect=MyExitError))
def test_choose_exceptions(test_config):
    test_config.init_local_files = False
    test_config.task_types = [mc.TaskType.SCRAPE]
    with pytest.raises(mc.CadcException):
        test_organizer = ec.OrganizeExecutes(
            test_config, 'command name', [], []
        )
        test_organizer.choose(tc.TestStorageName())


@patch('sys.exit', Mock(side_effect=MyExitError))
def test_storage_name_failure(test_config):
    class TestStorageNameFails(tc.TestStorageName):

        def __init__(self):
            super(TestStorageNameFails, self).__init__()

        def is_valid(self):
            return False
    test_config.log_to_file = True
    assert not os.path.exists(test_config.success_fqn)
    assert not os.path.exists(test_config.failure_fqn)
    assert not os.path.exists(test_config.retry_fqn)
    test_organizer = ec.OrganizeExecutes(
        test_config, 'command name', [], []
    )
    test_organizer.choose(TestStorageNameFails())
    assert os.path.exists(test_config.success_fqn)
    assert os.path.exists(test_config.failure_fqn)
    assert os.path.exists(test_config.retry_fqn)


def test_organize_executes_client_do_one(test_config):
    test_obs_id = tc.TestStorageName()
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
    exec_cmd_orig = mc.exec_cmd_info
    repo_client_mock = Mock(autospec=True)
    repo_client_mock.read.return_value = None

    try:
        mc.exec_cmd_info = Mock(
            return_value='INFO:cadc-data:info\n'
                         'File C170324_0054_SCI_prev.jpg:\n'
                         '    archive: OMM\n'
                         '   encoding: None\n'
                         '    lastmod: Mon, 25 Jun 2018 16:52:07 GMT\n'
                         '     md5sum: f37d21c53055498d1b5cb7753e1c6d6f\n'
                         '       name: C120902_sh2-132_J_old_'
                         'SCIRED.fits.gz\n'
                         '       size: 754408\n'
                         '       type: image/jpeg\n'
                         '    umd5sum: 704b494a972eed30b18b817e243ced7d\n'
                         '      usize: 754408\n'.encode('utf-8')
        )

        test_config.task_types = [mc.TaskType.SCRAPE]
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            chooser=None,
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 1
        assert isinstance(executors[0], ec.ScrapeUpdate)

        test_config.task_types = [
            mc.TaskType.STORE, mc.TaskType.INGEST, mc.TaskType.MODIFY
        ]
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            chooser=None,
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 3
        assert isinstance(executors[0], ec.Store), type(executors[0])
        assert isinstance(executors[1], ec.LocalMetaCreate)
        assert isinstance(executors[2], ec.LocalDataVisit)
        assert repo_client_mock.read.called, 'mock should be called'
        assert repo_client_mock.read.reset()

        test_config.use_local_files = False
        test_config.task_types = [mc.TaskType.INGEST, mc.TaskType.MODIFY]
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            chooser=None,
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 2
        assert isinstance(executors[0], ec.MetaCreate)
        assert isinstance(executors[1], ec.DataVisit)
        assert repo_client_mock.read.called, 'mock should be called'

        test_config.use_local_files = True
        test_config.task_types = [mc.TaskType.INGEST, mc.TaskType.MODIFY]
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            chooser=None,
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 2
        assert isinstance(executors[0], ec.LocalMetaCreate)
        assert isinstance(executors[1], ec.LocalDataVisit)
        assert repo_client_mock.read.called, 'mock should be called'
        assert repo_client_mock.read.reset()
        repo_client_mock.read.side_effect = _read_obs2

        test_config.task_types = [mc.TaskType.SCRAPE, mc.TaskType.MODIFY]
        test_config.use_local_files = True
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            chooser=None,
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 2
        assert isinstance(executors[0], ec.ScrapeUpdate)
        assert isinstance(executors[1], ec.DataScrape)
        assert repo_client_mock.read.called, 'mock should be called'
        assert repo_client_mock.read.reset()

        test_config.task_types = [mc.TaskType.INGEST]
        test_config.use_local_files = False
        test_chooser = tc.TestChooser()
        ec.CaomExecute.repo_cmd_get_client = Mock(return_value=_read_obs(None))
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            chooser=test_chooser,
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 1
        assert isinstance(executors[0], ec.MetaDeleteCreate)
        assert repo_client_mock.read.called, 'mock should be called'
        assert repo_client_mock.read.reset()

        test_config.task_types = [mc.TaskType.INGEST_OBS]
        test_config.use_local_files = False
        ec.CaomExecute.repo_cmd_get_client = Mock(
            return_value=_read_obs(test_obs_id)
        )
        test_oe = ec.OrganizeExecutes(
            test_config,
            TEST_APP,
            [],
            [],
            cadc_client=Mock(autospec=True),
            caom_client=repo_client_mock,
        )
        executors = test_oe.choose(test_obs_id)
        assert executors is not None
        assert len(executors) == 1
        assert isinstance(executors[0], ec.MetaUpdateObservation)
        assert repo_client_mock.read.called, 'mock should be called'
        assert executors[0].url == 'https://test_url/', 'url'
        assert executors[0].fname is None, 'file name'
        assert executors[0].stream == 'TEST', 'stream'
        assert executors[0].lineage is None, 'lineage'
        assert executors[0].external_urls_param == '', 'external_url_params'
        assert (
            executors[0].working_dir == f'{tc.THIS_DIR}/test_obs_id'
        ), 'working_dir'
        assert test_oe.todo_fqn == f'{tc.THIS_DIR}/todo.txt', 'wrong todo'
    finally:
        mc.exec_cmd_orig = exec_cmd_orig


@patch('caom2pipe.manage_composable.data_get')
def test_data_visit(get_mock, test_config):
    get_mock.side_effect = Mock(autospec=True)
    test_data_client = Mock(autospec=True)
    test_repo_client = Mock(autospec=True)
    test_repo_client.read.side_effect = tc.mock_read
    dv_mock = Mock(autospec=True)
    test_data_visitors = [dv_mock]
    test_observable = Mock(autospec=True)
    test_sn = mc.StorageName(
        obs_id='test_obs_id', collection='TEST', collection_pattern='T[\\w+-]+'
    )
    test_cred_param = ''
    test_transferrer = transfer_composable.CadcTransfer()
    test_transferrer.cadc_client = test_data_client
    test_transferrer.observable = test_observable

    test_subject = ec.DataVisit(
        test_config,
        test_sn,
        test_data_client,
        test_repo_client,
        test_data_visitors,
        mc.TaskType.VISIT,
        test_observable,
        test_transferrer,
    )
    test_subject.execute(None)
    assert get_mock.called, 'should be called'
    args, kwargs = get_mock.call_args
    assert args[1] == f'{tc.THIS_DIR}/test_obs_id', 'wrong directory'
    assert args[2] == 'test_obs_id.fits', 'wrong file name'
    assert args[3] == 'TEST', 'wrong archive'
    test_repo_client.read.assert_called_with(
        'OMM', 'test_obs_id'
    ), 'wrong values'
    assert test_repo_client.update.called, 'expect an execution'
    # TODO - why is the log file directory NOT the working directory?

    args, kwargs = dv_mock.visit.call_args
    assert kwargs.get('working_directory') == f'{tc.THIS_DIR}/test_obs_id'
    assert kwargs.get('science_file') == 'test_obs_id.fits'
    assert kwargs.get('log_file_directory') == tc.TEST_DATA_DIR
    assert kwargs.get('stream') == 'TEST'


def test_store(test_config):
    test_config.working_directory = tc.TEST_DATA_DIR
    test_sn = tc.TestStorageName()
    test_sn.source_names = ['vos:goliaths/nonexistent.fits.gz']
    test_command = 'collection2caom2'
    test_data_client = Mock(autospec=True)
    test_observable = Mock(autospec=True)
    test_transferrer = Mock(autospec=True)
    test_transferrer.get.side_effect = _transfer_get_mock
    test_subject = ec.Store(
        test_config,
        test_sn,
        test_command,
        test_data_client,
        test_observable,
        test_transferrer,
    )
    assert test_subject is not None, 'expect construction'
    assert test_subject.working_dir == os.path.join(
        tc.TEST_DATA_DIR, 'test_obs_id'
    ), 'wrong working directory'
    assert len(test_subject._destination_uris) == 1, 'wrong file count'
    assert len(test_subject._destination_f_names) == 1, \
        'wrong destination file count'
    assert (
        test_subject._destination_f_names[0] == 'nonexistent.fits.gz'
    ), 'wrong destination'
    test_subject.execute(None)
    assert test_data_client.put_file.called, 'data put not called'
    assert (
        test_data_client.put_file.call_args.args[0] == 'TEST'
    ), 'archive not set for test_config, is set for TestStorageName'
    assert (
        test_data_client.put_file.call_args.args[1] == os.path.join(
            tc.TEST_DATA_DIR, 'test_obs_id/nonexistent.fits.gz',
        )
    ), 'expect a fully-qualified file name'
    assert (
        test_data_client.put_file.call_args.kwargs['archive_stream'] == 'TEST'
    ), 'wrong archive stream'
    assert (
        test_data_client.put_file.call_args.kwargs['mime_type'] ==
        'application/fits'
    ), 'wrong archive'
    assert (
        test_data_client.put_file.call_args.kwargs['mime_encoding'] is None
    ), 'wrong archive'
    assert (
        test_data_client.put_file.call_args.kwargs['md5_check'] is True
    ), 'wrong archive'


def test_local_store(test_config):
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.data_source = ['/test_files/caom2pipe']
    test_config.use_local_files = True
    test_config.store_newer_files_only = False
    test_config.features.supports_latest_client = False
    test_sn = tc.TestStorageName()

    if not os.path.exists(test_sn.source_names[0]):
        with open(test_sn.source_names[0], 'w') as f:
            f.write('test content')

    test_command = 'collection2caom2'
    test_data_client = Mock(autospec=True)
    test_observable = Mock(autospec=True)
    test_subject = ec.LocalStore(
        test_config, test_sn, test_command, test_data_client, test_observable,
    )
    assert test_subject is not None, 'expect construction'
    test_subject.execute(None)
    # does the working directory get used if it's just a local store?
    assert (
        test_subject.working_dir == os.path.join(
            tc.TEST_DATA_DIR, 'test_obs_id')
    ), 'wrong working directory'
    # do one file at a time, so it doesn't matter how many files are
    # in the working directory
    assert len(test_subject._destination_uris) == 1, 'wrong file count'
    assert (
        len(test_subject._destination_f_names) == 1
    ), 'wrong destination file count'
    assert (
        test_subject._destination_f_names[0] == 'test_file.fits.gz'
    ), 'wrong destination'
    assert test_data_client.put_file.called, 'data put not called'
    assert test_data_client.put_file.call_args.args[0] == 'TEST', 'archive'
    assert (
        test_data_client.put_file.call_args.args[1] == test_sn.source_names[0]
    ), 'expect a file name'
    assert (
        test_data_client.put_file.call_args.kwargs['archive_stream'] == 'TEST'
    ), 'wrong archive'
    assert (
        test_data_client.put_file.call_args.kwargs['mime_type'] ==
        'application/fits'
    ), 'wrong archive'
    assert (
        test_data_client.put_file.call_args.kwargs['mime_encoding'] is None
    ), 'wrong archive'
    assert (
        test_data_client.put_file.call_args.kwargs['md5_check'] is True
    ), 'wrong archive'


class FlagStorageName(mc.StorageName):

    def __init__(self, file_name, source_names):
        super(FlagStorageName, self).__init__(
            fname_on_disk=file_name,
            obs_id='1000003f',
            collection='TEST',
            archive='TEST',
            compression='',
            entry=file_name,
            source_names=source_names,
        )
        self._file_name = file_name

    @property
    def file_name(self):
        return self._file_name


@patch('cadcdata.CadcDataClient')
def test_store_newer_files_only_flag(client_mock, test_config):
    # first test case
    # flag set to True, file is older at CADC, supports_latest_client = False
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.store_newer_files_only = True
    test_config.features.supports_latest_client = False
    test_f_name = '1000003f.fits.fz'
    sn = [os.path.join('/caom2pipe_test', test_f_name)]
    test_sn = FlagStorageName(test_f_name, sn)
    observable_mock = Mock(autospec=True)
    client_mock.get_file_info.return_value = {
        'lastmod': 'Mon, 4 Mar 2019 19:05:41 GMT'}

    test_subject = ec.LocalStore(
        test_config, test_sn, 'TEST_STORE', client_mock, observable_mock,
    )
    test_subject.execute(None)
    assert client_mock.put_file.called, 'expect put call'

    # second test case, flag set to True, file is newer at CADC
    client_mock.put_file.reset()
    client_mock.get_file_info.return_value = {
        'lastmod': datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
    }

    test_subject = ec.LocalStore(
        test_config, test_sn, 'TEST_STORE', client_mock, observable_mock,
    )
    test_subject.execute(None)
    assert client_mock.put_file.called, 'expect put call, file time is newer'
    client_mock.get_file_info.assert_called_with(
        None, '1000003f.fits.fz'
    ), 'wrong get_file_info call args'

    # third test case, flag set to False, file is older
    test_config.store_newer_files_only = False
    client_mock.put_file.reset()
    test_subject = ec.LocalStore(
        test_config, test_sn, 'TEST_STORE', client_mock, observable_mock,
    )
    test_subject.execute(None)
    assert client_mock.put_file.called, 'expect put call, file time irrelevant'


@patch('caom2pipe.manage_composable.client_put_fqn')
@patch('vos.Client')
def test_store_newer_files_only_flag_client(
        client_mock, put_mock, test_config
):
    # just like the previous test, except supports_latest_client = True
    # first test case
    # flag set to True, file is older at CADC, supports_latest_client = False
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.use_local_files = True
    test_config.store_newer_files_only = True
    test_config.features.supports_latest_client = True
    test_f_name = '1000003f.fits.fz'
    sn = [os.path.join('/caom2pipe_test', test_f_name)]
    test_sn = FlagStorageName(test_f_name, sn)
    observable_mock = Mock(autospec=True)
    test_node = type('', (), {})()
    test_node.props = {'date': 'Mon, 4 Mar 2019 19:05:41 GMT'}
    client_mock.get_node.return_value = test_node

    test_subject = ec.LocalStore(
        test_config, test_sn, 'TEST_STORE', client_mock, observable_mock,
    )
    test_subject.execute(None)
    assert put_mock.called, 'expect copy call'

    # second test case, flag set to True, file is newer at CADC
    put_mock.reset()
    test_node.props = {
        'date': datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
    }
    client_mock.get_node.return_value = test_node

    test_subject = ec.LocalStore(
        test_config, test_sn, 'TEST_STORE', client_mock, observable_mock,
    )
    test_subject.execute(None)
    assert put_mock.called, 'expect copy call, file time is newer'

    # third test case, flag set to False, file is older
    test_config.store_newer_files_only = False
    put_mock.reset()
    test_subject = ec.LocalStore(
        test_config, test_sn, 'TEST_STORE', client_mock, observable_mock,
    )
    test_subject.execute(None)
    assert put_mock.called, 'expect copy call, file time irrelevant'


def _transfer_get_mock(entry, fqn):
    assert entry == 'vos:goliaths/nonexistent.fits.gz', 'wrong entry'
    assert (
        fqn == os.path.join(
            tc.TEST_DATA_DIR, 'test_obs_id/nonexistent.fits.gz'
        )
    ), 'wrong fqn'
    with open(fqn, 'w') as f:
        f.write('test content')


def _communicate():
    return ['return status', None]


def _get_headers(uri, subject):
    x = """SIMPLE  =                    T / Written by IDL:  Fri Oct  6 01:48:35 2017
BITPIX  =                  -32 / Bits per pixel
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
        algorithm=Algorithm(str('exposure')),
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


def to_caom2():
    plugin = '/usr/local/lib/python3.8/site-packages/test_execute_composable/' \
             'test_execute_composable.py'
    assert sys.argv is not None, 'expect sys.argv to be set'
    local_meta_create_answer = [
        'test_execute_composable',
        '--verbose',
        '--observation',
        'OMM',
        'test_obs_id',
        '--local',
        '/usr/src/app/caom2pipe/caom2pipe/tests/data/test_file.fits.gz',
        '--out',
        '/usr/src/app/caom2pipe/caom2pipe/tests/test_obs_id/'
        'test_obs_id.fits.xml',
        '--plugin',
        f'{plugin}',
        '--module',
        f'{plugin}',
        '--lineage',
        'test_obs_id/ad:TEST/test_obs_id.fits.gz',
    ]
    scrape_answer = [
        'test_execute_composable',
        '--verbose',
        '--not_connected',
        '--observation',
        'OMM',
        'test_obs_id',
        '--local',
        '/usr/src/app/caom2pipe/caom2pipe/tests/data/test_file.fits.gz',
        '--out',
        '/usr/src/app/caom2pipe/caom2pipe/tests/data/test_obs_id/'
        'test_obs_id.fits.xml',
        '--plugin',
        f'{plugin}',
        '--module',
        f'{plugin}',
        '--lineage',
        'test_obs_id/ad:TEST/test_obs_id.fits.gz',
    ]
    # TaskType.SCRAPE (Scrape)
    if sys.argv != scrape_answer:
        # TaskType.INGEST (LocalMetaCreate)
        if sys.argv != local_meta_create_answer:
            assert False, \
                f'wrong sys.argv values \n{sys.argv} ' \
                f'\n{local_meta_create_answer}'
    fqn_index = sys.argv.index('--out') + 1
    fqn = sys.argv[fqn_index]
    mc.write_obs_to_file(
        SimpleObservation(
            collection='test_collection',
            observation_id='test_obs_id',
            algorithm=Algorithm(str('exposure'))
        ),
        fqn,
    )


def _mock_get_file_info(archive, file_id):
    return {
        'size': 10290,
        'md5sum': 'md5:{}'.format(md5('-37'.encode()).hexdigest()),
        'type': 'image/jpeg',
        'name': file_id,
    }

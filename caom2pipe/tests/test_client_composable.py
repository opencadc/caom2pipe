# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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

import os
import pytest

from cadcutils import exceptions
from cadcdata import FileInfo
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc

from unittest.mock import Mock, patch

import test_conf as tc


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_clients(get_access_mock):
    get_access_mock.return_value = 'https://localhost'
    test_config = mc.Config()
    test_config.get_executors()
    test_config.resource_id = 'ivo://cadc.nrc.ca/test'
    test_config.tap_id = 'ivo://cadc.nrc.ca/test'
    test_subject = clc.ClientCollection(test_config)
    assert test_subject is not None, 'ctor failure'


@patch('cadcdata.storageinv.StorageInventoryClient')
@patch('caom2pipe.manage_composable.Metrics')
def test_client_put(mock_metrics, mock_client):
    if not os.path.exists(f'{tc.TEST_FILES_DIR}/TEST.fits'):
        with open(f'{tc.TEST_FILES_DIR}/TEST.fits', 'w') as f:
            f.write('test content')

    test_destination = 'TBD'
    mock_client.copy.return_value = 12
    clc.si_client_put(
        mock_client,
        os.path.join(tc.TEST_FILES_DIR, 'TEST.fits'),
        test_destination,
        metrics=mock_metrics,
    )
    test_fqn = os.path.join(tc.TEST_FILES_DIR, 'TEST.fits')
    mock_client.cadcput.assert_called_with(
        'TBD',
        src='/test_files/TEST.fits',
        replace=True,
        file_type='application/fits',
        file_encoding='',
        md5_checksum='9473fdd0d880a43c21b7778d34872157',
    ), 'mock not called'
    assert mock_metrics.observe.called, 'mock not called'
    args, kwargs = mock_metrics.observe.call_args
    assert args[2] == 12, 'wrong size'
    assert args[3] == 'cadcput', 'wrong endpoint'
    assert args[4] == 'si', 'wrong service'
    assert args[5] == 'TEST.fits', 'wrong id'


@patch('caom2pipe.manage_composable.Metrics')
def test_client_put_failure(mock_metrics):
    if not os.path.exists(f'{tc.TEST_FILES_DIR}/TEST.fits'):
        with open(f'{tc.TEST_FILES_DIR}/TEST.fits', 'w') as f:
            f.write('test content')

    mock_client = Mock()
    mock_client.cadcput.side_effect = (
        exceptions.UnexpectedException('error state')
    )
    test_destination = 'cadc:GEMINI/TEST.fits'
    with pytest.raises(mc.CadcException):
        clc.si_client_put(
            mock_client,
            os.path.join(tc.TEST_FILES_DIR, 'TEST.fits'),
            test_destination,
            metrics=mock_metrics,
        )
    test_fqn = os.path.join(tc.TEST_FILES_DIR, 'TEST.fits')
    mock_client.cadcput.assert_called_with(
        'cadc:GEMINI/TEST.fits',
        src='/test_files/TEST.fits',
        replace=True,
        file_type='application/fits',
        file_encoding='',
        md5_checksum='9473fdd0d880a43c21b7778d34872157',
    ), 'mock not called'
    assert mock_metrics.observe_failure.called, 'mock not called'


@patch('cadcdata.storageinv.StorageInventoryClient')
def test_client_get_failure(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    with pytest.raises(mc.CadcException):
        clc.si_client_get(
            mock_client,
            os.path.join(tc.TEST_DATA_DIR, 'TEST_get.fits'),
            'cadc:TEST/TEST_get.fits',
            test_metrics,
        )
    assert len(test_metrics.failures) == 1, 'wrong failures'
    assert test_metrics.failures['si']['cadcget']['TEST_get.fits'] == 1, 'count'


@patch('vos.vos.Client')
@patch('caom2pipe.manage_composable.Metrics')
def test_client_get(mock_metrics, mock_client):
    test_fqn = f'{tc.TEST_FILES_DIR}/TEST.fits'
    if os.path.exists(test_fqn):
        os.unlink(test_fqn)

    test_source = 'gemini:GEMINI/TEST.fits'
    mock_client.copy.side_effect = tc.mock_copy
    clc.client_get(
        mock_client,
        tc.TEST_FILES_DIR,
        'TEST.fits',
        test_source,
        metrics=mock_metrics,
    )
    mock_client.copy.assert_called_with(
        test_source, destination=test_fqn
    ), 'mock not called'
    assert mock_metrics.observe.called, 'mock not called'
    args, kwargs = mock_metrics.observe.call_args
    assert args[2] == 12, 'wrong size'
    assert args[3] == 'copy', 'wrong endpoint'
    assert args[4] == 'vos', 'wrong service'
    assert args[5] == 'TEST.fits', 'wrong id'


@patch('cadcdata.core.CadcDataClient')
def test_data_get(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    with pytest.raises(mc.CadcException):
        clc.data_get(
            mock_client,
            tc.TEST_DATA_DIR,
            'TEST_get.fits',
            'TEST',
            test_metrics,
        )
    assert len(test_metrics.failures) == 1, 'wrong failures'
    assert test_metrics.failures['data']['get']['TEST_get.fits'] == 1, 'count'


def test_define_subject():

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)

    try:
        test_config = mc.Config()
        test_config.get_executors()
        test_config.proxy_fqn = None
        test_config.netrc_file = 'test_netrc'
        test_netrc_fqn = os.path.join(
            test_config.working_directory, test_config.netrc_file
        )
        if not os.path.exists(test_netrc_fqn):
            with open(test_netrc_fqn, 'w') as f:
                f.write(
                    'machine www.example.com login userid password userpass'
                )

        test_subject = clc.define_subject(test_config)
        assert test_subject is not None, 'expect a netrc subject'
        test_config.netrc_file = 'nonexistent'
        test_subject = clc.define_subject(test_config)
        assert test_subject is None, 'expect no subject, cannot find content'
        test_config.netrc_file = None
        # proxy pre-condition
        test_config.proxy_fqn = f'{tc.TEST_DATA_DIR}/proxy.pem'

        if not os.path.exists(test_config.proxy_fqn):
            with open(test_config.proxy_fqn, 'w') as f:
                f.write('proxy content')

        test_subject = clc.define_subject(test_config)
        assert test_subject is not None, 'expect a proxy subject'
        test_config.proxy_fqn = '/nonexistent'
        with pytest.raises(mc.CadcException):
            clc.define_subject(test_config)

        test_config.proxy_fqn = None
        with pytest.raises(mc.CadcException):
            clc.define_subject(test_config)
    finally:
        os.getcwd = getcwd_orig


@patch('caom2utils.data_util.StorageClientWrapper')
@patch('caom2pipe.manage_composable.http_get')
def test_look_pull_and_put(http_mock, mock_client):
    test_storage_name = 'cadc:GEMINI/TEST.fits'
    mock_client.info.return_value = FileInfo(
        id=test_storage_name,
        size=1234,
        md5sum='9473fdd0d880a43c21b7778d34872157',
    )
    f_name = 'TEST.fits'
    url = f'https://localhost/{f_name}'
    test_config = mc.Config()
    test_config.observe_execution = True
    mock_client.info.return_value = None
    test_fqn = os.path.join(tc.TEST_FILES_DIR, f_name)
    clc.look_pull_and_put(
        test_storage_name,
        test_fqn,
        url,
        mock_client,
        'md5:01234',
    )
    mock_client.put.assert_called_with(
        tc.TEST_FILES_DIR, test_storage_name
    ), 'mock not called'
    http_mock.assert_called_with(url, test_fqn), 'http mock not called'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_create(mock_client):
    test_obs = mc.read_obs_from_file(tc.TEST_OBS_FILE)
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    clc.repo_create(mock_client, test_obs, test_metrics)

    mock_client.create.assert_called_with(test_obs), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'create' in test_metrics.history['caom2'], 'create'
    assert 'test_obs_id' in test_metrics.history['caom2']['create'], 'obs id'

    mock_client.reset_mock()
    mock_client.create.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        clc.repo_create(mock_client, test_obs, test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_get(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    clc.repo_get(mock_client, 'collection', 'test_obs_id', test_metrics)

    mock_client.read.assert_called_with(
        'collection', 'test_obs_id'
    ), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'read' in test_metrics.history['caom2'], 'create'
    assert 'test_obs_id' in test_metrics.history['caom2']['read'], 'obs id'

    mock_client.reset_mock()
    mock_client.read.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        clc.repo_get(mock_client, 'collection', 'test_obs_id', test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_update(mock_client):
    test_obs = mc.read_obs_from_file(tc.TEST_OBS_FILE)
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    clc.repo_update(mock_client, test_obs, test_metrics)

    mock_client.update.assert_called_with(test_obs), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'update' in test_metrics.history['caom2'], 'update'
    assert 'test_obs_id' in test_metrics.history['caom2']['update'], 'obs id'

    mock_client.reset_mock()
    mock_client.update.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        clc.repo_update(mock_client, test_obs, test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_delete(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    clc.repo_delete(mock_client, 'coll', 'test_id', test_metrics)

    mock_client.delete.assert_called_with('coll', 'test_id'), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'delete' in test_metrics.history['caom2'], 'delete'
    assert 'test_id' in test_metrics.history['caom2']['delete'], 'obs id'

    mock_client.reset_mock()
    mock_client.delete.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        clc.repo_delete(mock_client, 'coll', 'test_id', test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('cadcdata.storageinv.StorageInventoryClient')
@patch('caom2pipe.manage_composable.Metrics')
def test_si_client_get(mock_metrics, mock_client):
    test_fqn = f'{tc.TEST_FILES_DIR}/TEST.fits'
    if os.path.exists(test_fqn):
        os.unlink(test_fqn)

    test_source = 'gemini:GEMINI/TEST.fits'
    test_fqn = os.path.join(tc.TEST_FILES_DIR, 'TEST.fits')
    mock_client.cadcget.side_effect = tc.mock_si_get
    mock_client.info.return_value = FileInfo(
        test_source, md5sum='9473fdd0d880a43c21b7778d34872157'
    )
    clc.si_client_get(
        mock_client,
        test_fqn,
        test_source,
        metrics=mock_metrics,
    )
    mock_client.cadcget.assert_called_with(
        test_source, dest=test_fqn
    ), 'mock not called'
    assert mock_metrics.observe.called, 'mock not called'
    args, kwargs = mock_metrics.observe.call_args
    assert args[2] == 12, 'wrong size'
    assert args[3] == 'cadcget', 'wrong endpoint'
    assert args[4] == 'si', 'wrong service'
    assert args[5] == 'TEST.fits', 'wrong id'

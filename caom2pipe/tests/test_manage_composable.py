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

import os
import pytest

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from caom2 import ProductType, ReleaseType, Artifact, ChecksumURI
from caom2 import SimpleObservation
from caom2pipe import manage_composable as mc

import test_conf as tc

TEST_STATE_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_state.yml')
TEST_OBS_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_obs_id.fits.xml')
ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def test_read_obs():
    test_subject = mc.read_obs_from_file(TEST_OBS_FILE)
    assert test_subject is not None, 'expect a result'
    assert isinstance(test_subject, SimpleObservation), 'wrong read'


def test_read_from_file():
    test_subject = mc.read_from_file(TEST_OBS_FILE)
    assert test_subject is not None, 'expect a result'
    assert isinstance(test_subject, list), 'wrong type of result'
    assert len(test_subject) == 8, 'missed some content'
    assert test_subject[0].startswith('<?xml version'), 'read failed'


def test_build_uri():
    test_subject = mc.build_uri('archive', 'file_name.fits')
    assert test_subject is not None, 'expect a result'
    assert test_subject == 'ad:archive/file_name.fits', 'wrong result'


def test_query_endpoint():

    with patch('requests.Session.get') as session_get_mock:
        test_result = mc.query_endpoint('https://localhost', timeout=25)
        assert test_result is not None, 'expected result'
        assert session_get_mock.called, 'mock not called'
        session_get_mock.assert_called_with('https://localhost', timeout=25)


def test_config_class():
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)
    mock_root = '/usr/src/app/caom2pipe/caom2pipe/tests/data'
    test_config = mc.Config()
    test_config.get_executors()
    assert test_config is not None
    assert test_config.work_file == 'todo.txt'
    assert test_config.features is not None
    assert test_config.features.supports_composite is False
    assert test_config.working_directory == mock_root, 'wrong dir'
    assert test_config.work_fqn == '{}/todo.txt'.format(mock_root), 'work_fqn'
    assert test_config.netrc_file == '.netrc', 'netrc'
    assert test_config.archive == 'NEOSS', 'archive'
    assert test_config.collection == 'NEOSSAT', 'collection'
    assert test_config.log_file_directory == mock_root, 'logging dir'
    assert test_config.success_fqn == '{}/success_log.txt'.format(mock_root), \
        'success fqn'
    assert test_config.success_log_file_name == 'success_log.txt', \
        'success file'
    assert test_config.failure_fqn == '{}/failure_log.txt'.format(mock_root), \
        'failure fqn'
    assert test_config.failure_log_file_name == 'failure_log.txt', \
        'failure file'
    assert test_config.retry_file_name == 'retries.txt', 'retry file'
    assert test_config.retry_fqn == '{}/retries.txt'.format(mock_root), \
        'retry fqn'
    assert test_config.proxy_file_name == 'test_proxy.pem', 'proxy file name'
    assert test_config.proxy_fqn == '{}/test_proxy.pem'.format(mock_root), \
        'proxy fqn'
    assert test_config.state_file_name == 'state.yml', 'state file name'
    assert test_config.state_fqn == '{}/state.yml'.format(mock_root), \
        'state fqn'
    assert test_config.rejected_directory == tc.TEST_DATA_DIR, \
        'wrong rejected dir'
    assert test_config.rejected_file_name == 'rejected.yml', \
        'wrong rejected file'
    assert test_config.rejected_fqn == '{}/rejected.yml'.format(
        tc.TEST_DATA_DIR), 'wrong rejected fqn'


def test_exec_cmd():
    test_cmd = 'ls /abc'
    with pytest.raises(mc.CadcException):
        mc.exec_cmd(test_cmd)


def test_exec_cmd_redirect():
    fqn = os.path.join(tc.TEST_DATA_DIR, 'exec_cmd_redirect.txt')
    if os.path.exists(fqn):
        os.remove(fqn)

    test_cmd = 'ls'
    mc.exec_cmd_redirect(test_cmd, fqn)
    assert os.path.exists(fqn)
    assert os.stat(fqn).st_size > 0


def test_decompose_lineage():
    test_product_id = 'product_id'
    test_uri = 'ad:STARS/galaxies.fits.gz'
    test_lineage = '{}/{}'.format(test_product_id, test_uri)
    actual_product_id, actual_uri = mc.decompose_lineage(test_lineage)
    assert actual_product_id == test_product_id, 'expected {}'.format(
        test_product_id)
    assert actual_uri == test_uri, 'expected {}'.format(test_uri)

    with pytest.raises(mc.CadcException):
        mc.decompose_lineage('')


def test_read_csv_file():
    # bad read
    with pytest.raises(mc.CadcException):
        mc.read_csv_file(None)

    # good read
    test_file_name = os.path.join(tc.TEST_DATA_DIR, 'test_csv.csv')
    content = mc.read_csv_file(test_file_name)
    assert content is not None, 'empty results returned'
    assert len(content) == 1, 'missed the comment and the header'
    assert len(content[0]) == 24, 'missed the content'


def test_get_file_meta():
    # None
    with pytest.raises(mc.CadcException):
        mc.get_file_meta(None)

    # non-existent file
    fqn = os.path.join(tc.TEST_DATA_DIR, 'abc.txt')
    with pytest.raises(mc.CadcException):
        mc.get_file_meta(fqn)

    # empty file
    fqn = os.path.join(tc.TEST_DATA_DIR, 'todo.txt')
    if os.path.exists(fqn):
        os.unlink(fqn)
    open(fqn, 'w').close()
    result = mc.get_file_meta(fqn)
    assert result['size'] == 0, result['size']


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_read_file_list_from_archive(caps_mock, ad_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = \
        [b'<?xml version="1.0" encoding="UTF-8"?>\n'
         b'<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3" '
         b'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
         b'version="1.3">\n'
         b'<RESOURCE type="results">\n'
         b'<INFO name="QUERY_STATUS" value="OK" />\n'
         b'<INFO name="QUERY_TIMESTAMP" value="2019-11-27T00:07:08.736" />\n'
         b'<INFO name="QUERY" value="SELECT ingestDate, fileName&#xA;FROM '
         b'archive_files&#xA;WHERE archiveName = \'NEOSS\'&#xA;AND '
         b'fileName = \'xEOS_SCI_2019319035900.fits\'" />\n'
         b'<TABLE>\n'
         b'<FIELD name="ingestDate" datatype="char" arraysize="*" '
         b'xtype="timestamp">\n'
         b'<DESCRIPTION>file ingest date</DESCRIPTION>\n'
         b'</FIELD>\n'
         b'<FIELD name="fileName" datatype="char" arraysize="255*">\n'
         b'<DESCRIPTION>file name</DESCRIPTION>\n'
         b'</FIELD>\n'
         b'<DATA>\n'
         b'<TABLEDATA />\n'
         b'</DATA>\n'
         b'</TABLE>\n'
         b'<INFO name="QUERY_STATUS" value="OK" />\n'
         b'</RESOURCE>\n'
         b'</VOTABLE>\n']
    ad_mock.return_value.__enter__.return_value = response
    test_config = mc.Config()
    result = mc.read_file_list_from_archive(test_config, 'test_app_name',
                                            '2018-11-18T22:39:56.186443+00:00',
                                            '2018-11-19T22:39:56.186443+00:00')
    assert result is not None
    assert type(result) is list
    assert len(result) == 0


def test_write_to_file():
    content = ['a.txt', 'b.jpg', 'c.fits.gz']
    test_fqn = '{}/test_out.txt'.format(tc.TEST_DATA_DIR)
    if os.path.exists(test_fqn):
        os.remove(test_fqn)

    mc.write_to_file(test_fqn, '\n'.join(content))
    assert os.path.exists(test_fqn)


def test_get_lineage():
    result = mc.get_lineage('TEST_COLLECTION', 'TEST_PRODUCT_ID',
                            'TEST_FILE_NAME.fits')
    assert result == 'TEST_PRODUCT_ID/ad:TEST_COLLECTION/TEST_FILE_NAME.fits'


def test_get_artifact_metadata():
    test_fqn = os.path.join(tc.TEST_DATA_DIR, 'config.yml')
    test_uri = 'ad:TEST/config.yml'

    # wrong command line parameters
    with pytest.raises(mc.CadcException):
        mc.get_artifact_metadata(test_fqn, ProductType.WEIGHT,
                                 ReleaseType.META)

    # create action
    result = mc.get_artifact_metadata(test_fqn, ProductType.WEIGHT,
                                      ReleaseType.META, uri=test_uri)
    assert result is not None, 'expect a result'
    assert isinstance(result, Artifact), 'expect an artifact'
    assert result.product_type == ProductType.WEIGHT, 'wrong product type'
    assert result.content_length == 349, 'wrong length'
    assert result.content_checksum.uri == \
        'md5:fb28c7904ace71ba3c713fb796325318', 'wrong checksum'

    # update action
    result.content_checksum = ChecksumURI('md5:abc')
    result = mc.get_artifact_metadata(test_fqn, ProductType.WEIGHT,
                                      ReleaseType.META, artifact=result)
    assert result is not None, 'expect a result'
    assert isinstance(result, Artifact), 'expect an artifact'
    assert result.content_checksum.uri == \
        'md5:fb28c7904ace71ba3c713fb796325318', 'wrong checksum'


@patch('cadcdata.core.CadcDataClient')
@patch('caom2pipe.manage_composable.Metrics')
def test_data_put(mock_metrics, mock_client):
    mc.data_put(mock_client, tc.TEST_DATA_DIR, 'TEST.fits', 'TEST', 'default',
                metrics=mock_metrics)
    mock_client.put_file.assert_called_with(
        'TEST', 'TEST.fits', archive_stream='default',
        md5_check=True, mime_encoding=None, mime_type=None), 'mock not called'
    assert mock_metrics.observe.called, 'mock not called'
    args, kwargs = mock_metrics.observe.call_args
    assert args[2] == 0, 'wrong size'
    assert args[3] == 'put', 'wrong endpoint'
    assert args[4] == 'data', 'wrong service'
    assert args[5] == 'TEST.fits', 'wrong id'


@patch('cadcdata.core.CadcDataClient')
def test_data_get(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    with pytest.raises(mc.CadcException):
        mc.data_get(mock_client, tc.TEST_DATA_DIR, 'TEST_get.fits', 'TEST',
                    test_metrics)
    assert len(test_metrics.failures) == 1, 'wrong failures'
    assert test_metrics.failures['data']['get']['TEST_get.fits'] == 1, 'count'


def test_state():
    if os.path.exists(TEST_STATE_FILE):
        os.unlink(TEST_STATE_FILE)
    with open(TEST_STATE_FILE, 'w') as f:
        f.write('bookmarks:\n'
                '  gemini_timestamp:\n'
                '    last_record: 2019-07-23 20:52:03.524443\n'
                'context:\n'
                '  neossat_context:\n'
                '    - NEOSS\n'
                '    - 2020\n')

    with pytest.raises(mc.CadcException):
        test_subject = mc.State('nonexistent')

    test_subject = mc.State(TEST_STATE_FILE)
    assert test_subject is not None, 'expect result'
    test_result = test_subject.get_bookmark('gemini_timestamp')
    assert test_result is not None, 'expect content'
    assert isinstance(test_result, datetime)

    test_context = test_subject.get_context('neossat_context')
    assert test_context is not None, 'expect a result'
    assert isinstance(test_context, list), 'wrong return type'
    assert len(test_context) == 2, 'wrong return length'
    assert 'NEOSS' in test_context, 'wrong content'
    test_context.append('2019')

    test_subject.save_state('gemini_timestamp',
                            test_result + timedelta(3))
    test_subject.save_state('neossat_context', test_context)

    with open(TEST_STATE_FILE, 'r') as f:
        text = f.readlines()
        compare = ''.join(ii for ii in text)
        assert '2019-07-23' not in compare, 'content not updated'
        assert '2019' in compare, 'context content not updated'


def test_make_seconds():
    t1 = '2017-06-26T17:07:21.527+00'
    t1_dt = mc.make_seconds(t1)
    assert t1_dt is not None, 'expect a result'
    assert t1_dt == 1498496841.527, 'wrong result'

    t2 = '2017-07-26T17:07:21.527'
    t2_dt = mc.make_seconds(t2)
    assert t2_dt is not None, 'expect a result'
    assert t2_dt == 1501088841.527, 'wrong result'

    t3 = '16-Jul-2019 09:08'
    t3_dt = mc.make_seconds(t3)
    assert t3_dt is not None, 'expect a result'
    assert t3_dt == 1563268080.0, 'wrong result'


def test_increment_time():
    t1 = '2017-06-26T17:07:21.527'
    t1_dt = datetime.strptime(t1, mc.ISO_8601_FORMAT)
    result = mc.increment_time(t1_dt, 10)
    assert result is not None, 'expect a result'
    assert result == datetime(2017, 6, 26, 17, 17, 21, 527000),\
        'wrong result'

    t2 = '2017-07-26T17:07:21.527'
    t2_dt = datetime.strptime(t2, mc.ISO_8601_FORMAT)
    result = mc.increment_time(t2_dt, 5)
    assert result is not None, 'expect a result'
    assert result == datetime(2017, 7, 26, 17, 12, 21, 527000),\
        'wrong result'

    with pytest.raises(NotImplementedError):
        mc.increment_time(t2_dt, 23, '%f')


@patch('cadcdata.core.CadcDataClient')
@patch('caom2pipe.manage_composable.http_get')
def test_look_pull_and_put(http_mock, mock_client):
    stat_orig = os.stat
    os.stat = Mock()
    os.stat.return_value = Mock(st_size=1234)
    try:
        f_name = 'test_f_name.fits'
        url = 'https://localhost/{}'.format(f_name)
        test_config = mc.Config()
        test_config.observe_execution = True
        test_metrics = mc.Metrics(test_config)
        assert len(test_metrics.history) == 0, 'initial history conditions'
        assert len(test_metrics.failures) == 0, 'initial failure conditions'
        mc.look_pull_and_put(f_name, tc.TEST_DATA_DIR, url, 'TEST', 'default',
                             'application/fits', mock_client, 'md5:01234',
                             test_metrics)
        mock_client.put_file.assert_called_with(
            'TEST', f_name, archive_stream='default', md5_check=True,
            mime_encoding=None,
            mime_type='application/fits'), 'mock not called'
        http_mock.assert_called_with(
            url, os.path.join(tc.TEST_DATA_DIR, f_name)), 'http mock not called'
        assert len(test_metrics.history) == 1, 'history conditions'
        assert len(test_metrics.failures) == 0, 'failure conditions'
    finally:
        os.stat = stat_orig


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_create(mock_client):
    test_obs = mc.read_obs_from_file(TEST_OBS_FILE)
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    mc.repo_create(mock_client, test_obs, test_metrics)

    mock_client.create.assert_called_with(test_obs), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'create' in test_metrics.history['caom2'], 'create'
    assert 'test_obs_id' in test_metrics.history['caom2']['create'], 'obs id'

    mock_client.reset_mock()
    mock_client.create.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        mc.repo_create(mock_client, test_obs, test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_get(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    mc.repo_get(mock_client, 'collection', 'test_obs_id', test_metrics)

    mock_client.read.assert_called_with('collection', 'test_obs_id'), \
        'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'read' in test_metrics.history['caom2'], 'create'
    assert 'test_obs_id' in test_metrics.history['caom2']['read'], 'obs id'

    mock_client.reset_mock()
    mock_client.read.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        mc.repo_get(mock_client, 'collection', 'test_obs_id', test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_update(mock_client):
    test_obs = mc.read_obs_from_file(TEST_OBS_FILE)
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    mc.repo_update(mock_client, test_obs, test_metrics)

    mock_client.update.assert_called_with(test_obs), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'update' in test_metrics.history['caom2'], 'update'
    assert 'test_obs_id' in test_metrics.history['caom2']['update'], 'obs id'

    mock_client.reset_mock()
    mock_client.update.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        mc.repo_update(mock_client, test_obs, test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('caom2repo.core.CAOM2RepoClient')
def test_repo_delete(mock_client):
    test_config = mc.Config()
    test_config.observe_execution = True
    test_metrics = mc.Metrics(test_config)
    assert len(test_metrics.history) == 0, 'initial history conditions'
    assert len(test_metrics.failures) == 0, 'initial failure conditions'

    mc.repo_delete(mock_client, 'coll', 'test_id', test_metrics)

    mock_client.delete.assert_called_with('coll', 'test_id'), 'mock not called'
    assert len(test_metrics.history) == 1, 'history conditions'
    assert len(test_metrics.failures) == 0, 'failure conditions'
    assert 'caom2' in test_metrics.history, 'history'
    assert 'delete' in test_metrics.history['caom2'], 'delete'
    assert 'test_id' in test_metrics.history['caom2']['delete'], 'obs id'

    mock_client.reset_mock()
    mock_client.delete.side_effect = Exception('boo')
    with pytest.raises(mc.CadcException):
        mc.repo_delete(mock_client, 'coll', 'test_id', test_metrics)
    assert len(test_metrics.failures) == 1, 'should have failure counts'


@patch('requests.get')
def test_http_get(mock_req):
    # Response mock
    class Object(object):

        def __init__(self):
            self.headers = {'Date': 'Thu, 25 Jul 2019 16:10:02 GMT',
                            'Server': 'Apache',
                            'Content-Length': '4',
                            'Cache-Control': 'no-cache',
                            'Expired': '-1',
                            'Content-Disposition':
                                'attachment; filename="S20080610S0045.fits"',
                            'Connection': 'close',
                            'Content-Type': 'application/fits'}

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            return ['aaa'.encode(), 'bbb'.encode()]

        def __enter__(self):
            return self

        def __exit__(self, a, b, c):
            return None

    test_object = Object()
    mock_req.return_value = test_object
    # wrong size
    with pytest.raises(mc.CadcException) as ex:
        mc.http_get('https://localhost/index.html', '/tmp/abc')

    assert mock_req.called, 'mock not called'
    assert 'File size error' in repr(ex.value), 'wrong failure'
    mock_req.reset_mock()

    # wrong checksum
    test_object.headers['Content-Length'] = 6
    test_object.headers['Content-Checksum'] = 'md5:abc'
    with pytest.raises(mc.CadcException) as ex:
        mc.http_get('https://localhost/index.html', '/tmp/abc')

    assert mock_req.called, 'mock not called'
    assert 'File checksum error' in repr(ex.value), 'wrong failure'
    mock_req.reset_mock()

    # checks succeed
    test_object.headers['Content-Checksum'] = \
        '6547436690a26a399603a7096e876a2d'
    mc.http_get('https://localhost/index.html', '/tmp/abc')
    assert mock_req.called, 'mock not called'


def test_create_dir():
    test_f_name = f'{tc.TEST_DATA_DIR}/test_file_dir'
    if os.path.exists(test_f_name):
        if os.path.isdir(test_f_name):
            os.rmdir(test_f_name)
        else:
            os.unlink(test_f_name)

    with open(test_f_name, 'w') as f:
        f.write('test content')

    with pytest.raises(mc.CadcException):
        mc.create_dir(test_f_name)

    os.unlink(test_f_name)

    mc.create_dir(test_f_name)

    assert os.path.exists(test_f_name), 'should have been created'


class TestValidator(mc.Validator):

    def __init__(self, source_name, preview_suffix):
        super(TestValidator, self).__init__(
            source_name, preview_suffix=preview_suffix)

    def read_from_source(self):
        return {}

    def find_unaligned_dates(self):
        return []


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcdata.core.net.BaseWsClient.get')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_validator(caps_mock, ad_mock, tap_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    tap_response = Mock()
    tap_response.status_code = 200
    tap_response.iter_content.return_value = \
        [b'uri\n'
         b'ad:NEOSS/NEOS_SCI_2019213215700_cord.fits\n'
         b'ad:NEOSS/NEOS_SCI_2019213215700_cor.fits\n'
         b'ad:NEOSS/NEOS_SCI_2019213215700.fits\n']

    tap_mock.return_value.__enter__.return_value = tap_response
    ad_response = Mock()
    ad_response.status_code = 200
    ad_response.text = []
    ad_mock.return_value = ad_response

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)

    try:
        test_subject = TestValidator('TEST_SOURCE_NAME', 'png')
        test_destination_meta = test_subject._read_list_from_destination_meta()
        assert test_destination_meta is not None, 'expected result'
        assert len(test_destination_meta) == 3, 'wrong number of results'
        assert test_destination_meta[0] == \
            'NEOS_SCI_2019213215700_cord.fits', \
            f'wrong value format, should be just a file name, ' \
            f'{test_destination_meta[0]}'

        test_listing_fqn = f'{tc.TEST_DATA_DIR}/{mc.VALIDATE_OUTPUT}'
        if os.path.exists(test_listing_fqn):
            os.unlink(test_listing_fqn)

        test_source, test_meta, test_data = test_subject.validate()
        assert test_source is not None, 'expected source result'
        assert test_meta is not None, 'expected meta dest result'
        assert test_data is not None, 'expected data dest result'
        assert len(test_source) == 0, 'wrong number of source results'
        assert len(test_meta) == 3, 'wrong # of meta dest results'
        assert len(test_data) == 0, 'wrong # of meta dest results'
        assert os.path.exists(test_listing_fqn), 'should create file record'
    finally:
        os.getcwd = getcwd_orig


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_validator2(caps_mock, ad_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = \
        [b'ingestDate,fileName\n'
         b'2019-10-23T16:27:19.000,NEOS_SCI_2015347000000_clean.fits\n'
         b'2019-10-23T16:27:27.000,NEOS_SCI_2015347000000.fits\n'
         b'2019-10-23T16:27:33.000,NEOS_SCI_2015347002200_clean.fits\n'
         b'2019-10-23T16:27:40.000,NEOS_SCI_2015347002200.fits\n'
         b'2019-10-23T16:27:47.000,NEOS_SCI_2015347002500_clean.fits\n']
    ad_mock.return_value.__enter__.return_value = response
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)
    try:
        test_subject = TestValidator('TEST_SOURCE_NAME', 'png')
        test_destination_data = test_subject._read_list_from_destination_data()
        assert test_destination_data is not None, 'expected data result'
        assert len(test_destination_data) == 5, 'wrong number of data results'
        test_result = test_destination_data[1]
        assert test_result['fileName'] == 'NEOS_SCI_2015347000000.fits', \
            f'wrong value format, should be just a file name, ' \
            f'{test_result["fileName"]}'
        assert test_result['ingestDate'] == '2019-10-23T16:27:27.000', \
            f'wrong value format, should be a datetime value, ' \
            f'{test_result["ingestDate"]}'
    finally:
        os.getcwd = getcwd_orig


def test_define_subject():

    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)

    try:
        test_config = mc.Config()
        test_config.get_executors()
        test_config.proxy_fqn = None
        assert test_config.netrc_file is not None, 'netrc branch pre-condition'
        test_netrc_fqn = os.path.join(test_config.working_directory,
                                      test_config.netrc_file)
        if not os.path.exists(test_netrc_fqn):
            with open(test_netrc_fqn, 'w') as f:
                f.write(
                    'machine www.example.com login userid password userpass')

        test_subject = mc.define_subject(test_config)
        assert test_subject is not None, 'expect a netrc subject'
        test_config.netrc_file = 'nonexistent'
        test_subject = mc.define_subject(test_config)
        assert test_subject is None, 'expect no subject, cannot find content'
        test_config.netrc_file = None
        # proxy pre-condition
        test_config.proxy_fqn = f'{tc.TEST_DATA_DIR}/proxy.pem'

        if not os.path.exists(test_config.proxy_fqn):
            with open(test_config.proxy_fqn, 'w') as f:
                f.write('proxy content')

        test_subject = mc.define_subject(test_config)
        assert test_subject is not None, 'expect a proxy subject'
        test_config.proxy_fqn = '/nonexistent'
        with pytest.raises(mc.CadcException):
            mc.define_subject(test_config)

        test_config.proxy_fqn = None
        with pytest.raises(mc.CadcException):
            mc.define_subject(test_config)
    finally:
        os.getcwd = getcwd_orig


@patch('cadcutils.net.ws.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_query_tap(caps_mock, base_mock, test_config):
    caps_mock.return_value = 'https://localhost'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = \
        [b'count\n'
         b'3212556\n']
    base_mock.return_value.__enter__.return_value = response
    test_config.tap_id = 'https://cadc.nrc.ca/sc2tap'
    result = mc.query_tap('select count(*) from caom2.Observation',
                          test_config.proxy_fqn, test_config.resource_id)
    assert result is not None, 'expect a result'
    assert len(result) == 1, 'wrong amount of test data'
    assert result['count'] == 3212556, 'wrong test data'

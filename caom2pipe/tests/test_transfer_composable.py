# -*- coding: utf-8 -*-
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

import os
import pytest
from mock import patch, Mock

from caom2pipe import manage_composable as mc
from caom2pipe import transfer_composable as tc

import test_conf


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.manage_composable.data_get')
def test_cadc_transfer(data_get_mock, caps_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    data_get_mock.side_effect = Mock(autospec=True)
    test_config = mc.Config()
    test_config.working_directory = test_conf.TEST_DATA_DIR
    test_config.netrc_file = 'test_netrc'
    test_subject = tc.CadcTransfer(test_config)
    assert test_subject is not None, 'expect a result'
    test_source = 'ad:TEST/test_file.fits'
    test_destination = '/tmp/test_file.fits'
    test_subject.get(test_source, test_destination)
    assert data_get_mock.called, 'should have been called'
    args, kwargs = data_get_mock.call_args
    assert args[1] == '/tmp', 'wrong dir name'
    assert args[2] == 'test_file.fits', 'wrong file name'
    assert args[3] == 'TEST', 'wrong archive name'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('vos.Client.copy')
def test_vo_transfer(get_mock, caps_mock):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2tap'
    get_mock.side_effect = Mock(autospec=True)
    test_config = mc.Config()
    test_config.working_directory = test_conf.TEST_DATA_DIR
    test_config.proxy_file_name = 'proxy.pem'
    test_subject = tc.VoTransfer(test_config)
    assert test_subject is not None, 'expect a result'
    test_source = 'vault:goliaths/test_file.fits'
    test_dest = '/tmp/test_file.fits'
    test_subject.get(test_source, test_dest)
    assert get_mock.called, 'should have been called'
    args, kwargs = get_mock.call_args
    assert args[0] == test_source, 'wrong source'
    assert args[1] == test_dest, 'wrong source'


@patch('caom2pipe.manage_composable.http_get')
def test_http_transfer(get_mock):
    test_source = 'http://localhost/test_file.fits'
    test_destination = '/tmp/test_file.fits'
    if not os.path.exists(test_destination):
        with open(test_destination, 'w') as f:
            f.write('test content')
    get_mock.side_effect = Mock(autospec=True)
    test_config = mc.Config()
    test_config.working_directory = test_conf.TEST_DATA_DIR
    test_config.netrc_file = 'test_netrc'
    test_config.rejected_fqn = '/tmp/rejected.yml'
    test_observable = mc.Observable(
        mc.Rejected(test_config.rejected_fqn), mc.Metrics(test_config))
    test_subject = tc.HttpTransfer(test_observable)
    assert test_subject is not None, 'expect a result'
    with pytest.raises(mc.CadcException):
        test_subject.get(test_source, test_destination)
        assert get_mock.called, 'should have been called'
        args, kwargs = get_mock.call_args
        assert args[1] == test_source, 'wrong source name'
        assert args[2] == test_destination, 'wrong dest name'


@patch('caom2pipe.manage_composable.ftp_get_timeout')
def test_ftp_transfer(data_get_mock):
    test_source = 'ftp://localhost/test_file.fits'
    test_destination = '/tmp/test_file.fits'
    if not os.path.exists(test_destination):
        with open(test_destination, 'w') as f:
            f.write('test content')
    data_get_mock.side_effect = Mock(autospec=True)
    test_config = mc.Config()
    test_config.working_directory = test_conf.TEST_DATA_DIR
    test_config.netrc_file = 'test_netrc'
    test_config.rejected_fqn = '/tmp/rejected.yml'
    test_observable = mc.Observable(
        mc.Rejected(test_config.rejected_fqn), mc.Metrics(test_config))
    test_subject = tc.FtpTransfer('localhost', test_observable)
    assert test_subject is not None, 'expect a result'
    with pytest.raises(mc.CadcException):
        test_subject.get(test_source, test_destination)
        assert data_get_mock.called, 'should have been called'
        args, kwargs = data_get_mock.call_args
        assert args[1] == 'localhost', 'wrong dir name'
        assert args[2] == test_source, 'wrong source name'
        assert args[3] == test_destination, 'wrong dest name'

# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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

from caom2pipe.validator_composable import VALIDATE_OUTPUT, Validator
from unittest.mock import Mock, patch


class TestValidator(Validator):
    def __init__(self, source_name, preview_suffix):
        super().__init__(source_name, preview_suffix=preview_suffix)

    def read_from_source(self):
        import pandas as pd
        return pd.DataFrame({'f_name': [], 'timestamp': []})

    def _find_unaligned_dates(self, source, data):
        import pandas as pd
        return pd.DataFrame({'f_name': []})


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcdata.core.net.BaseWsClient.get')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_validator(caps_mock, ad_mock, tap_mock, test_config, tmpdir):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    tap_response = Mock()
    tap_response.status_code = 200
    tap_response.iter_content.return_value = [
        b'uri\tcontentLastModified\n'
        b'cadc:NEOSSAT/NEOS_SCI_2019213215700_cord.fits\t2017-12-12 00:00:00.000\n'
        b'cadc:NEOSSAT/NEOS_SCI_2019213215700_cor.fits\t2017-12-12 00:00:00.001\n'
        b'cadc:NEOSSAT/NEOS_SCI_2019213215700.fits\t2017-12-12 00:00:00.002\n'
    ]

    tap_mock.return_value.__enter__.return_value = tap_response
    storage_response = Mock()
    storage_response.status_code = 200
    storage_response.text = [b'uri\tcontentLastModified\n']
    ad_mock.return_value = storage_response
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        test_config.change_working_directory(tmpdir)
        test_config.proxy_file_name = 'proxy.pem'
        test_config.write_to_file(test_config)
        with open(test_config.proxy_fqn, 'w') as f:
            f.write('test content')

        test_subject = TestValidator('TEST_SOURCE_NAME', 'png')
        test_destination_meta = (
            test_subject._read_list_from_destination_meta()
        )
        assert test_destination_meta is not None, 'expected result'
        assert len(test_destination_meta) == 3, 'wrong number of results'
        assert (
            test_destination_meta.loc[0, 'f_name'] == 'NEOS_SCI_2019213215700_cord.fits'
        ), (
            f'wrong value format, should be just a file name, '
            f'{test_destination_meta.loc[0, "f_name"]}'
        )

        test_source, test_meta, test_data = test_subject.validate()
        assert test_source is not None, 'expected source result'
        assert test_meta is not None, 'expected meta dest result'
        assert test_data is not None, 'expected data dest result'
        assert len(test_source) == 0, 'wrong number of source results'
        assert len(test_meta) == 3, 'wrong # of meta dest results'
        assert len(test_data) == 0, 'wrong # of meta dest results'
        test_listing_fqn = f'{tmpdir}/{VALIDATE_OUTPUT}'
        assert os.path.exists(test_listing_fqn), 'should create file record'
    finally:
        os.chdir(orig_cwd)


@patch('cadcdata.core.net.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_validator2(caps_mock, storage_mock, test_config, tmpdir):
    caps_mock.return_value = 'https://sc2.canfar.net/sc2repo'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = [
        b'uri\tcontentLastModified\n'
        b'cadc:NEOSSAT/NEOS_SCI_2015347000000_clean.fits\t2019-10-23T16:27:19.000\n'
        b'cadc:NEOSSAT/NEOS_SCI_2015347000000.fits\t2019-10-23T16:27:27.000\n'
        b'cadc:NEOSSAT/NEOS_SCI_2015347002200_clean.fits\t2019-10-23T16:27:33.000\n'
        b'cadc:NEOSSAT/NEOS_SCI_2015347002200.fits\t2019-10-23T16:27:40.000\n'
        b'cadc:NEOSSAT/NEOS_SCI_2015347002500_clean.fits\t2019-10-23T16:27:47.000\n'
    ]
    storage_mock.return_value.__enter__.return_value = response
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        test_config.change_working_directory(tmpdir)
        test_config.proxy_file_name = 'proxy.pem'
        test_config.write_to_file(test_config)
        with open(test_config.proxy_fqn, 'w') as f:
            f.write('test content')

        test_subject = TestValidator('TEST_SOURCE_NAME', 'png')
        test_destination_data = (
            test_subject._read_list_from_destination_data()
        )
        assert test_destination_data is not None, 'expected data result'
        assert len(test_destination_data) == 5, 'wrong number of data results'
        test_result = test_destination_data.loc[1, :]
        assert (
            test_result.f_name == 'NEOS_SCI_2015347000000.fits'
        ), f'wrong value format, should be just a file name, {test_result.f_name}'
        assert (
            test_result.contentLastModified == '2019-10-23T16:27:27.000'
        ), f'should be a datetime value, {test_result.contentLastModified}'
    finally:
        os.chdir(orig_cwd)



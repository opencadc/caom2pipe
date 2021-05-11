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

import glob
import os
import pytest
import stat

from astropy.table import Table
from cadctap import CadcTapClient
from datetime import datetime, timedelta
from mock import Mock, patch

from caom2pipe import data_source_composable as dsc
from caom2pipe import manage_composable as mc

import test_conf as tc


def test_list_dir_data_source():
    get_cwd_orig = os.getcwd
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)
    test_config = mc.Config()
    test_config.get_executors()
    test_config.working_directory = '/test_files/1'

    if not os.path.exists(test_config.working_directory):
        os.mkdir(test_config.working_directory)
    os.chmod(
        test_config.working_directory,
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
    )

    for entry in ['TEST.fits.gz', 'TEST1.fits', 'TEST2.fits.fz', 'TEST3.hdf5']:
        if not os.path.exists(f'{test_config.working_directory}/{entry}'):
            with open(f'{test_config.working_directory}/{entry}', 'w') as f:
                f.write('test content')

    test_chooser = tc.TestChooser()
    try:
        test_subject = dsc.ListDirDataSource(test_config, test_chooser)
        test_result = test_subject.get_work()
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 4, 'wrong result'
        assert 'TEST.fits.gz' in test_result, 'wrong gz extensions'
        assert 'TEST1.fits.gz' in test_result, 'wrong no extension'
        assert 'TEST2.fits.fz' in test_result, 'wrong fz extensions'
        assert 'TEST3.hdf5' in test_result, 'wrong hdf5'

        test_subject = dsc.ListDirDataSource(test_config, chooser=None)
        test_result = test_subject.get_work()
        assert test_result is not None, 'expected a result'
        assert len(test_result) == 4, 'wrong result'
        assert 'TEST.fits.gz' in test_result, 'wrong gz extensions'
        assert 'TEST1.fits' in test_result, 'wrong no extension'
        assert 'TEST2.fits.fz' in test_result, 'wrong fz extensions'
        assert 'TEST3.hdf5' in test_result, 'wrong hdf5'

    finally:
        os.getcwd = get_cwd_orig

        if os.path.exists(test_config.working_directory):
            for entry in glob.glob(f'{test_config.working_directory}/*'):
                os.unlink(entry)
            os.rmdir(test_config.working_directory)


def test_todo_file():
    todo_fqn = os.path.join(tc.TEST_DATA_DIR, 'todo.txt')
    with open(todo_fqn, 'w') as f:
        f.write('file1\n')
        f.write('file2\n')
        f.write('\n')
    try:
        test_config = mc.Config()
        test_config.work_fqn = todo_fqn
        test_subject = dsc.TodoFileDataSource(test_config)
        test_result = test_subject.get_work()
        assert test_result is not None, 'expect result'
        assert len(test_result) == 2, 'wrong number of files'
    finally:
        if os.path.exists(todo_fqn):
            os.unlink(todo_fqn)


@patch('caom2pipe.manage_composable.query_tap_client')
def test_storage_time_box_query(query_mock):
    def _mock_query(arg1, arg2):
        return Table.read(
            'fileName,ingestDate\n'
            'NEOS_SCI_2015347000000_clean.fits,2019-10-23T16:27:19.000\n'
            'NEOS_SCI_2015347000000.fits,2019-10-23T16:27:27.000\n'
            'NEOS_SCI_2015347002200_clean.fits,2019-10-23T16:27:33.000\n'.split(
                '\n'
            ),
            format='csv',
        )

    query_mock.side_effect = _mock_query
    getcwd_orig = os.getcwd
    os.getcwd = Mock(return_value=tc.TEST_DATA_DIR)
    tap_client_ctor_orig = CadcTapClient.__init__
    CadcTapClient.__init__ = Mock(return_value=None)
    test_config = mc.Config()
    test_config.get_executors()
    utc_now = datetime.utcnow()
    prev_exec_date = utc_now - timedelta(seconds=3600)
    exec_date = utc_now - timedelta(seconds=1800)
    try:
        test_subject = dsc.QueryTimeBoxDataSource(test_config)
        test_result = test_subject.get_time_box_work(prev_exec_date, exec_date)
        assert test_result is not None, 'expect result'
        assert len(test_result) == 3, 'wrong number of results'
        assert (
            test_result[0].entry_name == 'NEOS_SCI_2015347000000_clean.fits'
        ), 'wrong results'
    finally:
        os.getcwd = getcwd_orig
        CadcTapClient.__init__ = tap_client_ctor_orig


def test_vault_list_dir_data_source():
    def _query_mock(ignore_source_directory):
        return ['abc.txt', 'abc.fits']
    test_vos_client = Mock()
    test_vos_client.listdir.side_effect = _query_mock
    test_config = mc.Config()
    test_config.get_executors()
    test_config.data_source = 'vos:goliaths/wrong'
    test_subject = dsc.VaultListDirDataSource(test_vos_client, test_config)
    assert test_subject is not None, 'expect a test_subject'
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a test result'
    assert len(test_result) == 1, 'wrong number of results'
    assert 'vos:goliaths/wrong/abc.fits' in test_result, 'wrong result'

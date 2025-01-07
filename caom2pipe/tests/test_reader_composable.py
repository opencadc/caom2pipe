# -*- coding: utf-8 -*-
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

from mock import Mock
from os.path import basename
from caom2pipe import manage_composable as mc
from caom2pipe import reader_composable
import test_conf as tc


def test_file_reader(test_config):
    test_subject = reader_composable.FileMetadataReader()
    test_fqn = f'{tc.TEST_FILES_DIR}/correct.fits'
    test_uri = 'cadc:TEST/correct.fits'
    mc.StorageName.collection = 'TEST'
    test_storage_name = mc.StorageName(
        file_name='correct.fits',
        source_names=[test_fqn],
    )
    test_subject.set(test_storage_name)
    test_header_result = test_subject.headers
    assert test_header_result is not None, 'expect a header result'
    assert len(test_header_result) == 1, 'wrong headers'
    test_headers = test_header_result.pop(test_uri)
    assert len(test_headers) == 6, 'wrong header count'
    test_file_info_result = test_subject.file_info
    assert len(test_file_info_result) == 1, 'wrong file_info'
    test_file_info = test_file_info_result.pop(test_uri)
    assert test_file_info is not None, 'expect a result'
    assert test_file_info.id == basename(test_fqn), 'wrong uri'
    assert test_file_info.file_type == 'application/fits', 'wrong type'
    assert test_file_info.size == 197442, 'wrong size'
    assert test_file_info.md5sum == '053b0780633ebab084b19050c0a58620', 'wrong md5sum'
    test_subject.reset()
    assert len(test_subject.headers) == 0, 'should be no headers'
    assert len(test_subject.file_info) == 0, 'should be no file_info'


def test_factory(test_config):
    cfht_config = mc.Config()
    cfht_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    cfht_config.use_local_files = True

    dao_config = mc.Config()
    dao_config.use_local_files = False
    dao_config.data_sources = ['vos:goliaths/dao']

    vlass_config = mc.Config()
    vlass_config.use_local_files = False
    vlass_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST, mc.TaskType.MODIFY]
    vlass_config.data_sources = ['https://localhost:8080']

    fix_config = mc.Config()
    fix_config.use_local_files = False
    fix_config.task_types = [mc.TaskType.INGEST, mc.TaskType.MODIFY]

    for test_cfg, expected_type in {
        cfht_config: reader_composable.FileMetadataReader,
        dao_config: reader_composable.VaultReader,
        vlass_config: reader_composable.DelayedClientReader,
        fix_config: reader_composable.StorageClientReader,
    }.items():
        result = reader_composable.reader_factory(test_cfg, Mock())
        assert isinstance(result, expected_type), f'got {result} type instead'

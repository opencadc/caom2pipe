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
import logging
import os

from mock import Mock, patch
import test_conf as tc

from caom2 import SimpleObservation, Algorithm
from caom2pipe import data_source as ds
from caom2pipe import builder as b
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import run_composable as rc


TEST_COMMAND = 'test_command'
TEST_DIR = f'{tc.TEST_DATA_DIR}/run_composable'
TEST_SOURCE = '{}/test_command/test_command.py'.format(
    distutils.sysconfig.get_python_lib())


@patch('caom2pipe.execute_composable.CaomExecute._fits2caom2_cmd_local_direct')
def test_run_list_dir_data_source(fits2caom2_mock, test_config):
    test_config.working_directory = TEST_DIR
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]

    test_builder = b.StorageNameInstanceBuilder(test_config.collection)
    test_chooser = ec.OrganizeChooser()
    test_data_source = ds.ListDirDataSource(test_config, test_chooser)
    test_organizer = ec.OrganizeExecutesWithDoOne(
            test_config, TEST_COMMAND, [], [], test_chooser)
    test_subject = rc.TodoRunner(test_config, test_organizer,
                                 test_builder, test_data_source, test_chooser)
    test_result = test_subject.run()
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert fits2caom2_mock.called, 'expect fits2caom2 call'


@patch('caom2pipe.execute_composable.CAOM2RepoClient')
def test_run_todo_file_data_source(repo_mock, test_config):
    repo_mock.return_value.read.side_effect = Mock(
        return_value=SimpleObservation(collection=test_config.collection,
                                       observation_id='def',
                                       algorithm=Algorithm(str('test'))))
    test_config.work_fqn = f'{TEST_DIR}/todo.txt'
    test_config.task_types = [mc.TaskType.VISIT]
    test_builder = b.StorageNameInstanceBuilder(test_config.collection)
    test_chooser = ec.OrganizeChooser()
    test_data_source = ds.TodoFileDataSource(test_config)
    test_organizer = ec.OrganizeExecutesWithDoOne(
            test_config, TEST_COMMAND, [], [], test_chooser)
    test_subject = rc.TodoRunner(test_config, test_organizer,
                                 test_builder, test_data_source, test_chooser)
    test_result = test_subject.run()
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'

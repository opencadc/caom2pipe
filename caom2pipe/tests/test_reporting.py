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


from caom2pipe import data_source_composable as dsc
from caom2pipe.manage_composable import TaskType
from caom2pipe import run_composable as rc

from unittest.mock import call, Mock, patch
from test_data_source_composable import _create_dir_listing


@patch('caom2pipe.client_composable.ClientCollection')
def test_report_output_todo_local(clients_mock, test_config):
    # work is all the list of files, some of which have been stored already, and some of which fail fitsverify

    test_config.task_types = [TaskType.STORE]
    test_config.use_local_files = True
    test_config.data_sources = ['/tmp']
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'

    pre_success_listing, file_info_list = _create_dir_listing('/tmp', 3)
    test_reader = Mock()
    test_reader.file_info.get.side_effect = [file_info_list[0], None, None]
    clients_mock.return_value.data_client.info.side_effect = [file_info_list[0], None, None]

    test_end_time = None

    for clean_up in [True, False]:
        for store_modified in [True, False]:
            test_config.cleanup_files_when_storing = clean_up
            test_config.store_modified_files_only = store_modified
            test_data_source = dsc.LocalFilesDataSource(
                test_config, clients_mock.return_value.data_client, test_reader, recursive=True, scheme='cadc'
            )
            for state in [True, False]:
                (
                    test_config,
                    clients_mock,
                    test_builder,
                    test_data_source,
                    test_modify_transfer,
                    test_reader,
                    test_store_transfer,
                    test_organizer,
                ) = rc.common_runner_init(
                    config=test_config,
                    clients=clients_mock,
                    name_builder=None,
                    source=test_data_source,
                    modify_transfer=None,
                    metadata_reader=test_reader,
                    state=state,
                    store_transfer=None,
                    meta_visitors=[],
                    data_visitors=[],
                    chooser=None,
                )
                check_mock = Mock()
                # FITS files are valid, invalid, valid
                check_mock.side_effect = [True, False, True]
                test_data_source._check_file = check_mock
                move_mock = Mock()
                test_data_source._move_action = move_mock
                if state:
                    test_subject = rc.StateRunner(
                        test_config,
                        test_organizer,
                        test_builder,
                        test_data_source,
                        test_reader,
                        'TEST_BOOKMARK',
                        'DEFAULT',
                        test_end_time,
                    )
                else:
                    test_subject = rc.TodoRunner(
                        test_config, test_organizer, test_builder, test_data_source, test_reader, 'DEFAULT'
                    )

                diagnostic = f'clean up {clean_up} store modified {store_modified} subject {type(test_subject)}'
                with patch('os.scandir') as scandir_mock:
                    scandir_mock.return_value.__enter__.return_value = pre_success_listing
                    test_result = test_subject.run()
                    assert test_result is not None, f'expect a result {diagnostic}'
                    assert test_result == 0, f'expect success {diagnostic}'
                    assert test_subject._organizer._capture_failure.called, f'expect failure count {diagnostic}'
                    assert (
                        test_subject._organizer._capture_failure.call_count == 1
                    ), f'wrong capture failure call count {diagnostic}'
                    assert test_subject._organizer._capture_success.called, f'expect success count {diagnostic}'
                    assert (
                        test_subject._organizer._capture_success.call_count == 1
                    ), f'wrong capture success call count {diagnostic}'
                    assert move_mock.called, f'move should be called {diagnostic}'
                    assert move_mock.call_count == 2, f'wrong move call count {diagnostic}'
                    move_mock.assert_has_calls(
                        [
                            call('/tmp/A0.fits', '/data/success'),
                            call('/tmp/A1.fits', '/data/failure'),
                        ]
                    ), f'wrong move_mock calls {diagnostic}'
                    test_report = test_subject._report
                    assert test_report._entries_sum == 3, f'entries {diagnostic}'
                    assert test_report._success_sum == 1, f'success {diagnostic}'
                    assert test_report._timeouts_sum == 0, f'timeouts {diagnostic}'
                    assert test_report._retry_sum == 0, f'retry {diagnostic}'
                    assert test_report._errors_sum == 1, f'errors {diagnostic}'
                    assert test_report._rejection_sum == 0, f'rejection {diagnostic}'
                    assert test_report._disregarded_sum == 1, f'disregarded {diagnostic}'


@patch('caom2pipe.client_composable.ClientCollection')
def test_report_output_todo_vo(clients_mock, test_config):
    # work is all the list of files, some of which have been stored already, and some of which fail fitsverify

    test_config.task_types = [TaskType.STORE]
    test_config.use_local_files = True
    test_config.data_sources = ['/tmp']
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.store_modified_files_only = True

    pre_success_listing, file_info_list = _create_dir_listing('/tmp', 3)
    test_reader = Mock()
    test_reader.file_info.get.side_effect = [file_info_list[0], None, None]
    clients_mock.return_value.data_client.info.side_effect = [file_info_list[0], None, None]

    for clean_up in [True, False]:
        for store_modified in [True, False]:
            test_config.cleanup_files_when_storing = clean_up
            test_config.store_modified_files_only = store_modified
            test_local = dsc.LocalFilesDataSource(
                test_config, clients_mock.return_value.data_client, test_reader, recursive=True, scheme='cadc'
            )
            test_vo = dsc.LocalFilesDataSource(
                test_config, clients_mock.return_value.vo_client, clients_mock.return_value.data_client, scheme='cadc'
            )

            for test_data_source in [test_local, test_vo]:
                (
                    test_config,
                    clients_mock,
                    test_builder,
                    test_data_source,
                    test_modify_transfer,
                    test_reader,
                    test_store_transfer,
                    test_organizer,
                ) = rc.common_runner_init(
                    config=test_config,
                    clients=clients_mock,
                    name_builder=None,
                    source=test_data_source,
                    modify_transfer=None,
                    metadata_reader=test_reader,
                    state=False,
                    store_transfer=None,
                    meta_visitors=[],
                    data_visitors=[],
                    chooser=None,
                )
                check_mock = Mock()
                # FITS files are valid, invalid, valid
                check_mock.side_effect = [True, False, True]
                test_data_source._check_file = check_mock
                move_mock = Mock()
                test_data_source._move_action = move_mock
                test_subject = rc.TodoRunner(
                    test_config, test_organizer, test_builder, test_data_source, test_reader, 'TEST_APP'
                )

                with patch('os.scandir') as scandir_mock:
                    scandir_mock.return_value.__enter__.return_value = pre_success_listing
                    test_result = test_subject.run()
                    assert test_result is not None, f'expect a result {type(test_data_source)}'
                    assert test_result == 0, f'expect success {type(test_data_source)}'
                    assert test_subject.work_todo_count == 3, 'wrong count'
                    assert not move_mock.called, 'move should not yet be called'
                    test_subject.capture_clean_up()
                    assert test_subject._capture_failure.called, 'expect failure count'
                    assert test_subject._capture_failure.call_count == 1, 'wrong capture failure call count'
                    assert test_subject._capture_success.called, 'expect success count'
                    assert test_subject._capture_success.call_count == 1, 'wrong capture success call count'
                    assert move_mock.called, 'move should be called'
                    assert move_mock.call_count == 2, 'wrong move call count'
                    move_mock.assert_has_calls(
                        [
                            call('/tmp/A0.fits', '/data/success'),
                            call('/tmp/A1.fits', '/data/failure'),
                        ]
                    ), 'wrong move_mock calls'

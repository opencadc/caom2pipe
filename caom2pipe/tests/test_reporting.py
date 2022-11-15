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

"""
Integration testing that pytest makes easy, between:
 the run_composable (TodoRunner, StateRunner),
 data_source_composable (particularly LocalFilesDataSource and VaultCleanupDataSource), and
 manage_composable refactoring of ExecutionReporter and ExecutionSummary

from the previous locations across the DataSource specializations, the OrganizeExecutor, and the *Runner
specializations.
"""

from datetime import datetime
from tempfile import TemporaryDirectory

from cadcdata import FileInfo
from caom2pipe import data_source_composable as dsc
from caom2pipe.manage_composable import CadcException, Config, TaskType
from caom2pipe import run_composable as rc

from unittest.mock import call, Mock, patch
from test_data_source_composable import _create_dir_listing
from test_run_composable import TEST_BOOKMARK, _write_state


def test_report_output_todo_local(test_config):
    test_config.cleanup_files_when_storing = True
    test_config.store_modified_files_only = True
    # diagnostic = (
    #     f'clean up {test_config.cleanup_files_when_storing} store modified {test_config.store_modified_files_only} '
    #     f'subject {type(test_subject)}'
    # )
    # work is all the list of files,
    #   one has been stored already,
    #   one fails fitsverify
    #   one fails ingestion
    #   one succeeds

    test_config.logging_level = 'DEBUG'
    test_config.task_types = [TaskType.STORE]
    test_config.use_local_files = True
    test_config.data_sources = ['/tmp']
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.interval = 100

    pre_success_listing, file_info_list = _create_dir_listing('/tmp', 4)
    test_reader = Mock()
    # 0 stored already
    # 1 fails fitsverify
    # 2 fails ingestion
    # 3 succeeds
    different_file_info = FileInfo(id='A0.fits', md5sum='ghi')

    test_start_time = datetime(year=2020, month=3, day=3, hour=1, minute=1, second=1)
    test_end_time = datetime(year=2020, month=3, day=3, hour=2, minute=1, second=1)

    store_modified_true_move_calls = [
        call('/tmp/A0.fits', '/data/success'),
        call('/tmp/A1.fits', '/data/failure'),
        call('/tmp/A2.fits', '/data/failure'),
        call('/tmp/A3.fits', '/data/success'),
    ]

    store_modified_false_move_calls = [
        call('/tmp/A1.fits', '/data/failure'),
        call('/tmp/A0.fits', '/data/success'),
        call('/tmp/A2.fits', '/data/failure'),
        call('/tmp/A3.fits', '/data/success'),
    ]

    store_modified_true_put_side_effect = [CadcException, None]
    store_modified_false_put_side_effect = [None, CadcException, None]

    clean_up_false_put_side_effect = [None, CadcException, None]

    store_modified_true_info_side_effect = [file_info_list[0], None, None, None, file_info_list[0]]
    store_modified_false_info_side_effect = [
        file_info_list[0], None, file_info_list[0], file_info_list[0], file_info_list[0]
    ]

    store_modified_true_file_info_get_side_effect = [file_info_list[0], file_info_list[3], None, None]
    store_modified_false_file_info_get_side_effect = [file_info_list[0], file_info_list[2], file_info_list[3]]

    store_modified_true_skipped_sum = 1
    store_modified_false_skipped_sum = 0

    test_clients = Mock()
    with TemporaryDirectory() as temp_dir:
        test_config.working_directory = temp_dir
        test_config.log_file_directory = temp_dir
        test_config.state_fqn = f'{temp_dir}/{test_config.state_file_name}'
        test_config.log_file_directory = temp_dir
        test_config.failure_log_file_name = 'fail.txt'
        test_config.retry_file_name = 'do_again.txt'
        test_config.success_log_file_name = 'good.txt'
        test_config.progress_file_name = 'progress.txt'
        test_config.rejected_file_name = 'reject.txt'
        test_config._report_fqn = f'{temp_dir}/report.txt'

        for clean_up in [True, False]:
            for store_modified in [True, False]:
                move_calls = store_modified_false_move_calls
                put_side_effect = store_modified_false_put_side_effect
                info_side_effect = store_modified_false_info_side_effect
                file_info_side_effect = store_modified_false_file_info_get_side_effect
                skipped_sum = store_modified_false_skipped_sum
                if store_modified:
                    move_calls = store_modified_true_move_calls
                    put_side_effect = store_modified_true_put_side_effect
                    info_side_effect = store_modified_true_info_side_effect
                    file_info_side_effect = store_modified_true_file_info_get_side_effect
                    skipped_sum = store_modified_true_skipped_sum
                if not clean_up:
                    put_side_effect = clean_up_false_put_side_effect
                    skipped_sum = store_modified_false_skipped_sum
                test_config.cleanup_files_when_storing = clean_up
                test_config.store_modified_files_only = store_modified
                Config.write_to_file(test_config)
                test_data_source = dsc.LocalFilesDataSource(
                    test_config, test_clients.data_client, test_reader, recursive=True, scheme='cadc'
                )
                _write_state(test_start_time, test_config.state_fqn)
                for state in [True, False]:
                    # import logging
                    # logging.error('')
                    # logging.error('')
                    # logging.error('')
                    # logging.error('')
                    (
                        test_config,
                        test_clients,
                        test_builder,
                        test_data_source,
                        test_reader,
                        test_organizer,
                        test_observable,
                        test_reporter,
                    ) = rc.common_runner_init(
                        config=test_config,
                        clients=test_clients,
                        name_builder=None,
                        source=test_data_source,
                        modify_transfer=None,
                        metadata_reader=test_reader,
                        state=state,
                        store_transfer=None,
                        meta_visitors=[],
                        data_visitors=[],
                        chooser=None,
                        application='DEFAULT',
                    )
                    # 0 stored already
                    # 1 fails fitsverify
                    # 2 fails ingestion
                    # 3 succeeds
                    test_clients.data_client.info.side_effect = info_side_effect
                    test_clients.data_client.put.side_effect = put_side_effect
                    verify_mock = Mock()
                    # FITS files are valid, invalid, valid, valid
                    verify_mock.side_effect = [True, False, True, True]
                    test_reader.file_info.get.side_effect = file_info_side_effect
                    test_data_source._verify_file = verify_mock
                    if state:
                        test_subject = rc.StateRunner(
                            test_config,
                            test_organizer,
                            test_builder,
                            test_data_source,
                            test_reader,
                            TEST_BOOKMARK,
                            test_observable,
                            test_reporter,
                            test_end_time,
                        )
                    else:
                        test_subject = rc.TodoRunner(
                            test_config,
                            test_organizer,
                            test_builder,
                            test_data_source,
                            test_reader,
                            test_observable,
                            test_reporter,
                        )

                    diagnostic = f'clean up {clean_up} store modified {store_modified} subject {type(test_subject)}'
                    _y(test_subject, pre_success_listing, move_calls, skipped_sum, clean_up, diagnostic)


def _y(test_subject, pre_success_listing, move_calls, skipped_sum, clean_up, diagnostic):
    move_mock = Mock()
    test_subject._data_source._move_action = move_mock

    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = pre_success_listing
        try:
            test_result = test_subject.run()
        except Exception as e:
            import logging
            import traceback
            logging.error(traceback.format_exc())
            assert False, f'{e} {diagnostic}'

        assert test_result is not None, f'expect a result {diagnostic}'
        assert test_result == -1, f'there are some failures {diagnostic}'
        if clean_up:
            assert move_mock.called, f'move should be called {diagnostic}'
            assert move_mock.call_count == 4, f'wrong move call count {diagnostic}'
            move_mock.assert_has_calls(move_calls)
        else:
            assert not move_mock.called, f'move should not be called {diagnostic}'
        test_report = test_subject._data_source._reporter._summary
        assert test_report._entries_sum == 4, f'entries {diagnostic} {test_report.report()}'
        assert test_report._success_sum == 2, f'success {diagnostic} {test_report.report()}'
        assert test_report._timeouts_sum == 0, f'timeouts {diagnostic} {test_report.report()}'
        assert test_report._retry_sum == 0, f'retry {diagnostic} {test_report.report()}'
        assert test_report._errors_sum == 2, f'errors {diagnostic} {test_report.report()}'
        assert test_report._rejected_sum == 1, f'rejection {diagnostic} {test_report.report()}'
        assert test_report._skipped_sum == skipped_sum, f'skipped {diagnostic} {test_report.report()}'


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
                test_data_source._verify_file = check_mock
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

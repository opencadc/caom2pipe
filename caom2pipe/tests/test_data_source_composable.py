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
import shutil
import stat

from astropy.table import Table
from cadctap import CadcTapClient
from datetime import datetime, timedelta, timezone
from pathlib import Path
from mock import Mock, patch

from cadcdata import FileInfo
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
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
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


@patch('caom2pipe.client_composable.query_tap_client')
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
        return ['abc.txt', 'abc.fits', '900898p_moc.fits']

    node1 = type('', (), {})()
    node1.props = {
        'size': 0,
    }
    node1.uri = 'vos://cadc.nrc.ca!vault/goliaths/wrong/900898p_moc.fits'
    node2 = type('', (), {})()
    node2.props = {
        'size': 12,
    }
    node2.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/abc.fits'
    node3 = type('', (), {})()
    node3.props = {
        'size': 12,
    }
    node3.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/abc.txt'

    test_vos_client = Mock()
    test_vos_client.listdir.side_effect = _query_mock
    test_vos_client.get_node.side_effect = [node1, node2, node3]
    test_config = mc.Config()
    test_config.get_executors()
    test_config.data_sources = ['vos:goliaths/wrong']
    test_config.data_source_extensions = ['.fits']
    test_subject = dsc.VaultDataSource(test_vos_client, test_config)
    assert test_subject is not None, 'expect a test_subject'
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a test result'
    assert len(test_result) == 1, 'wrong number of results'
    assert 'vos:goliaths/wrong/abc.fits' in test_result, 'wrong result'


def test_list_dir_time_box_data_source():
    test_prev_exec_time_dt = datetime.utcnow()

    test_dir = '/test_files/1'
    import time

    time.sleep(1)
    test_sub_dir = '/test_files/1/sub_directory'
    test_file_1 = os.path.join(test_dir, 'abc1.fits')
    test_file_2 = os.path.join(test_sub_dir, 'abc2.fits')

    # so the timestamps are correct, delete then re-create the test
    # directory structure
    if os.path.exists(test_file_2):
        os.unlink(test_file_2)
        os.rmdir(test_sub_dir)
    if os.path.exists(test_file_1):
        os.unlink(test_file_1)
        os.rmdir(test_dir)

    for entry in [test_dir, test_sub_dir]:
        os.mkdir(entry)

    for entry in [test_file_1, test_file_2]:
        with open(entry, 'w') as f:
            f.write('test content')

    test_config = mc.Config()
    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.data_sources = [test_dir]
    test_config.data_source_extensions = ['.fits']

    try:
        test_subject = dsc.ListDirTimeBoxDataSource(test_config)
        assert test_subject is not None, 'ctor is broken'
        test_prev_exec_time = test_prev_exec_time_dt.timestamp()
        test_exec_time_dt = datetime.utcnow()
        test_exec_time = test_exec_time_dt.timestamp() + 3600.0
        test_result = test_subject.get_time_box_work(
            test_prev_exec_time, test_exec_time
        )
        assert test_result is not None, 'expect a result'
        assert len(test_result) == 2, 'expect contents in the result'
        test_entry = test_result.popleft()
        assert (
            test_entry.entry_name == test_file_1
        ), 'wrong expected file, order matters since should be sorted deque'

        test_subject = dsc.ListDirTimeBoxDataSource(
            test_config, recursive=False
        )
        test_result = test_subject.get_time_box_work(
            test_prev_exec_time, test_exec_time
        )
        assert test_result is not None, 'expect a non-recursive result'
        assert len(test_result) == 1, 'expect contents in non-recursive result'
        x = [ii.entry_name for ii in test_result]
        assert test_file_2 not in x, 'recursive result should not be present'
    finally:
        if os.path.exists(test_file_2):
            os.unlink(test_file_2)
            os.rmdir(test_sub_dir)
        if os.path.exists(test_file_1):
            os.unlink(test_file_1)
            os.rmdir(test_dir)


def test_list_dir_separate_data_source():
    test_config = mc.Config()
    test_config.data_sources = ['/test_files']
    test_config.data_source_extensions = [
        '.fits',
        '.fits.gz',
        '.fits.fz',
        '.hdf5',
    ]
    test_subject = dsc.ListDirSeparateDataSource(test_config)
    assert test_subject is not None, 'ctor is broken'
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a result'
    assert len(test_result) == 76, 'expect contents in the result'
    assert '/test_files/sub_directory/abc.fits' in test_result, 'wrong entry'

    test_subject = dsc.ListDirSeparateDataSource(test_config, recursive=False)
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a non-recursive result'
    assert len(test_result) == 74, 'expect contents in non-recursive result'
    assert (
        '/test_files/sub_directory/abc.fits' not in test_result
    ), 'recursive result should not be present'


def test_vault_list_dir_time_box_data_source():
    node1 = type('', (), {})()
    node1.props = {
        'date': '2020-09-15 19:55:03.067000+00:00',
        'size': 14,
    }
    node1.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/994898p_moc.fits'
    node2 = type('', (), {})()
    node2.props = {
        'date': '2020-09-13 19:55:03.067000+00:00',
        'size': 12,
    }
    node2.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/994899p_moc.fits'
    node1.isdir = Mock(return_value=False)
    node2.isdir = Mock(return_value=False)

    def _glob_mock(ignore_source_directory):
        return [1, 2]

    def _get_node_mock(target):
        if target == 1:
            return node1
        else:
            return node2

    test_vos_client = Mock()
    test_vos_client.glob.side_effect = _glob_mock
    test_vos_client.get_node.side_effect = _get_node_mock
    test_config = mc.Config()
    test_config.get_executors()
    test_config.data_sources = ['vos:goliaths/wrong']
    test_subject = dsc.VaultDataSource(test_vos_client, test_config)
    assert test_subject is not None, 'expect a test_subject'
    test_prev_exec_time = datetime(
        year=2020,
        month=9,
        day=15,
        hour=10,
        minute=0,
        second=0,
        tzinfo=timezone.utc,
    )
    test_exec_time = datetime(
        year=2020,
        month=9,
        day=16,
        hour=10,
        minute=0,
        second=0,
        tzinfo=timezone.utc,
    )
    test_result = test_subject.get_time_box_work(
        test_prev_exec_time, test_exec_time
    )
    assert test_result is not None, 'expect a test result'
    assert len(test_result) == 1, 'wrong number of results'
    assert (
            'vos://cadc.nrc.ca!vault/goliaths/moc/994898p_moc.fits' ==
            test_result[0].entry_name
    ), 'wrong name result'
    assert (
            datetime(2020, 9, 15, 19, 55, 3, 67000, tzinfo=timezone.utc) ==
            test_result[0].entry_ts
    ), 'wrong ts result'


def test_transfer_check_fits_verify():
    # how things should probably work at CFHT
    delta = timedelta(minutes=30)
    # half an hour ago
    test_start_time = datetime.now(tz=timezone.utc) - delta
    # half an hour from now
    test_end_time = test_start_time + delta + delta
    test_source_directory = Path('/cfht_source')
    test_failure_directory = Path('/cfht_transfer_failure')
    test_success_directory = Path('/cfht_transfer_success')
    test_empty_file = Path('/cfht_source/empty_file.fits.fz')
    test_broken_file = Path('/cfht_source/broken.fits')
    test_broken_source = Path('/test_files/broken.fits')
    test_correct_file = Path('/cfht_source/correct.fits.gz')
    test_correct_source = Path('/test_files/correct.fits.gz')
    test_dot_file = Path('/cfht_source/.dot_file.fits')
    test_same_source = Path('/test_files/same_file.fits')
    test_same_file = Path('/cfht_source/same_file.fits')
    test_already_successful = Path('/cfht_source/already_successful.fits')
    test_already_successful_source = Path(
        '/test_files/already_successful.fits'
    )

    def mock_info(uri):
        return FileInfo(
            id=uri,
            size=12,
            md5sum='e4e153121805745792991935e04de322',
        )

    def _at_cfht(test_start_ts, test_end_ts):
        test_config.cleanup_files_when_storing = True
        test_config.retry_failures = False
        cadc_client_mock = Mock(autospec=True)
        cadc_client_mock.info.side_effect = mock_info
        test_subject = dsc.UseLocalFilesDataSource(
            test_config, cadc_client_mock
        )

        assert test_subject is not None, 'expect construction to work'
        test_result = test_subject.get_time_box_work(test_start_ts, test_end_ts)
        assert len(test_result) == 1, 'wrong number of results returned'
        assert (
                test_result[0].entry_name == '/cfht_source/correct.fits.gz'
        ), 'wrong result'

        for entry in [test_empty_file, test_broken_file]:
            assert not entry.exists(), 'file at source'
            moved = Path(test_failure_directory, entry.name)
            assert moved.exists(), 'file at destination'

        # same file has been moved
        assert not test_same_file.exists(), 'same file at source'
        moved = Path(test_success_directory, test_same_file.name)
        assert moved.exists(), 'same file at destination'

        # correct file - the file stays where it is until it's transferred
        assert test_correct_file.exists(), 'correct file at source'
        moved_success = Path(test_success_directory, test_correct_file.name)
        moved_failure = Path(test_failure_directory, test_correct_file.name)
        assert not moved_success.exists(), 'correct file at success'
        assert not moved_failure.exists(), 'correct file at failure'

        # and after the transfer
        for test_entry in test_result:
            test_subject.clean_up(test_entry, current_count=0)
        assert not test_correct_file.exists(), 'correct file at source'
        assert not moved_success.exists(), 'correct file at destination'
        assert moved_failure.exists(), 'correct file at destination'

        # dot file - which should be ignored
        assert test_dot_file.exists(), 'correct file at source'
        moved = Path(test_success_directory, test_dot_file.name)
        assert not moved.exists(), 'dot file at destination'

    def _at_cadc(test_start_ts, test_end_ts):
        test_config.cleanup_files_when_storing = False
        test_config.features.supports_latest_client = True
        cadc_client_mock = Mock(autospec=True)
        cadc_client_mock.info.side_effect = mock_info
        test_subject = dsc.UseLocalFilesDataSource(
            test_config, cadc_client_mock
        )
        assert test_subject is not None, 'expect construction to work'
        test_result = test_subject.get_time_box_work(
            test_start_ts, test_end_ts
        )
        assert len(test_result) == 1, 'wrong number of results returned'
        assert (
                test_result[0].entry_name == '/cfht_source/correct.fits.gz'
        ), 'wrong result'
        for f in [
            test_empty_file,
            test_broken_file,
            test_correct_file,
            test_dot_file,
            test_same_file,
        ]:
            assert f.exists(), 'file at source'
            moved = Path(test_failure_directory, f.name)
            assert not moved.exists(), 'file at destination'
        # clean up should do nothing
        for test_entry in test_result:
            test_subject.clean_up(test_entry, current_count=0)
        for f in [
            test_empty_file,
            test_broken_file,
            test_correct_file,
            test_dot_file,
            test_same_file,
        ]:
            assert f.exists(), 'file at source'
            moved = Path(test_failure_directory, f.name)
            assert not moved.exists(), 'file at destination'

    def _move_failure(test_start_ts, test_end_ts):
        def _move_mock():
            raise mc.CadcException('move mock')

        move_orig = shutil.move
        shutil.move = Mock(side_effect=_move_mock)
        try:
            test_config.cleanup_files_when_storing = True
            cadc_client_mock = Mock(autospec=True)
            cadc_client_mock.info.side_effect = mock_info
            test_subject = dsc.UseLocalFilesDataSource(
                test_config, cadc_client_mock
            )
            with pytest.raises(mc.CadcException):
                test_result = test_subject.get_time_box_work(
                    test_start_ts, test_end_ts
                )
        finally:
            shutil.move = move_orig

    for test in [_at_cfht, _at_cadc, _move_failure]:
        for entry in [
            test_failure_directory,
            test_success_directory,
            test_source_directory
        ]:
            if not entry.exists():
                entry.mkdir()
            for child in entry.iterdir():
                child.unlink()
        for entry in [test_empty_file, test_dot_file]:
            entry.touch()
        for source in [
            test_broken_source,
            test_correct_source,
            test_same_source,
            test_already_successful_source,
        ]:
            shutil.copy(source, test_source_directory)

        # CFHT test case - try to move a file that would have the effect
        # of replacing a file already in the destination directory
        shutil.copy(test_already_successful_source, test_success_directory)

        test_config = mc.Config()
        test_config.use_local_files = True
        test_config.data_sources = [test_source_directory.as_posix()]
        test_config.data_source_extensions = ['.fits', '.fits.gz', '.fits.fz']
        test_config.cleanup_success_destination = \
            test_success_directory.as_posix()
        test_config.cleanup_failure_destination = \
            test_failure_directory.as_posix()
        test_config.store_modified_files_only = True

        test(
            test_start_time.timestamp(),
            test_end_time.timestamp(),
        )


@patch('caom2pipe.astro_composable.check_fits')
def test_transfer_fails(check_fits_mock):
    check_fits_mock.return_value = True

    # set up a correct transfer
    test_source_directory = Path('/cfht_source')
    test_failure_directory = Path('/cfht_transfer_failure')
    test_success_directory = Path('/cfht_transfer_success')
    test_correct_file_1 = Path('/cfht_source/correct_1.fits.gz')
    test_correct_file_2 = Path('/cfht_source/correct_2.fits.gz')
    test_correct_source = Path('/test_files/correct.fits.gz')

    for entry in [
        test_failure_directory,
        test_success_directory,
        test_source_directory
    ]:
        if not entry.exists():
            entry.mkdir()
        for child in entry.iterdir():
            child.unlink()
    shutil.copy(test_correct_source, test_correct_file_1)
    shutil.copy(test_correct_source, test_correct_file_2)

    test_config = mc.Config()
    test_config.data_sources = [test_source_directory.as_posix()]
    test_config.data_source_extensions = ['.fits.gz']
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = test_failure_directory.as_posix()
    test_config.cleanup_success_destination = test_success_directory.as_posix()

    cadc_client_mock = Mock(autospec=True)
    test_subject = dsc.UseLocalFilesDataSource(
        test_config, cadc_client_mock
    )
    assert test_subject is not None, 'ctor failure'
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a result'
    assert len(test_result) == 2, 'wrong number of entries in result'

    match = FileInfo(
        id=test_correct_file_1.as_posix(),
        size=12,
        md5sum='dfb5ddab74844d911dd54552415dd8ab',
    )
    different = FileInfo(
        id=test_correct_file_2.as_posix(),
        size=12,
        md5sum='e4e153121805745792991935e04de322',
    )

    cadc_client_mock.info.side_effect = [match, different]
    for test_entry in test_result:
        test_subject.clean_up(test_entry, current_count=0)

    assert not test_correct_file_1.exists(), 'file 1 should be moved'
    assert not test_correct_file_2.exists(), 'file 2 should be moved'
    moved_1 = Path(test_success_directory, test_correct_file_1.name)
    moved_2 = Path(test_failure_directory, test_correct_file_2.name)
    assert moved_1.exists(), 'file 1 not moved to correct location'
    assert moved_2.exists(), 'file 2 not moved to correct location'

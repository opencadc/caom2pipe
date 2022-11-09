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

import logging
import os
import shutil
import traceback

from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime
from dateutil import tz

from cadctap import CadcTapClient
from cadcutils import exceptions
from caom2pipe import astro_composable as ac
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc

__all__ = [
    'DataSource',
    'data_source_factory',
    'DecompressionDataSource',
    'ListDirDataSource',
    'ListDirSeparateDataSource',
    'ListDirTimeBoxDataSource',
    'LocalFilesDataSource',
    'QueryTimeBoxDataSource',
    'RenameURIDataSource',
    'StateRunnerMeta',
    'TodoFileDataSource',
    'VaultDataSource',
]


class DataSource:
    """
    The mechanisms for identifying the lists of work to be done (otherwise
    known as which class instantiates the extension of the DataSource class)
    are:
    - a todo.txt file, where every entry in the file is a unit of work
    - a directory listing (i.e. ls('./dir/*.fz')), where every file listed
        is a unit of work
    - a time-boxed listing (based on bookmarks in a state.yml file) - an
        incremental approach, where the work to be done is chunked (by
        time-boxes for now, as it's the only in-use requirement)
    - a time-boxed directory listing
    - an implementation of the work_composable.work class, which returns a
        deque of work todo as a method implementation
    """

    def __init__(self, config=None):
        self._config = config
        # if this value is used, it should be a timestamp - i.e. a float
        self._start_time_ts = None
        self._extensions = None
        if config is not None:
            self._extensions = config.data_source_extensions
        self._capture_failure = None
        self._capture_success = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def clean_up(self, entry, execution_result, current_count):
        pass

    def get_work(self):
        return deque()

    def get_time_box_work(self, prev_exec_time, exec_time):
        return deque()

    @property
    def capture_failure(self):
        return self._capture_failure

    @capture_failure.setter
    def capture_failure(self, value):
        self._capture_failure = value

    @property
    def capture_success(self):
        return self._capture_success

    @capture_success.setter
    def capture_success(self, value):
        self._capture_success = value

    @property
    def start_time_ts(self):
        return self._start_time_ts

    @start_time_ts.setter
    def start_time_ts(self, value):
        self._start_time_ts = value

    def default_filter(self, entry):
        """
        :param entry: os.DirEntry
        """
        for extension in self._extensions:
            if entry.name.endswith(extension):
                return True
        return False


class DecompressionDataSource(DataSource):
    """
    Implement a Data Source that queries for files in Storage Inventory
    that are compressed, where there does not exist a corresponding
    decompressed copy of the file.

    For CFHT, the condition is where there does not exist a corresponding
    .fz compressed copy of the file.
    """

    def __init__(self, config, collection, scheme, suffix='.gz'):
        super().__init__(config)
        self._suffix = suffix
        subject = clc.define_subject(config)
        # the resource_id values are hard-coded - if the referenced instances
        # of the services aren't available, the query answers are just
        # considered not available.
        self._luskan_client = CadcTapClient(
            subject, resource_id='ivo://cadc.nrc.ca/global/luskan'
        )
        self._compressed = None
        self._decompressed = None
        self._collection = collection
        self._scheme = scheme
        self._logger = logging.getLogger(self.__class__.__name__)

    def _match_work(self):
        new_extension = ''
        if self._collection == 'CFHT':
            new_extension = '.fz'
        self._compressed.uri = self._compressed.uri.astype(str).replace(
            self._suffix, new_extension
        )
        return self._compressed[
            ~self._compressed.uri.isin(self._decompressed.uri)
        ]

    def _query_by_extension(self, extension):
        # a TAP query to find all the files in SI with a particular extension
        qs = f"""
        SELECT A.uri
        FROM inventory.Artifact AS A
        WHERE A.uri like '{self._scheme}:{self._collection}/%{extension}'
        """
        return clc.query_tap_client(qs, self._luskan_client).to_pandas()

    def get_cleanup_work(self):
        return self._query_by_extension(
            f'.fits{self._suffix}'
        ).uri.values.tolist()[1:]

    def get_work(self):
        self._compressed = self._query_by_extension(f'.fits{self._suffix}')
        self._logger.info(
            f'Found {self._compressed.shape[0]} SI records with '
            f'extension .fits{self._suffix}.'
        )
        self._decompressed = self._query_by_extension('.fits')
        self._logger.info(
            f'Found {self._decompressed.shape[0]} SI records with '
            f'extension .fits.'
        )
        # make a list for ray, skip the first entry, which is the text header
        # string 'uri'
        return self._match_work().uri.values.tolist()[1:]


class ListDirDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing. This is the original use_local_files: True behaviour.
    """

    def __init__(self, config, chooser):
        super().__init__(config)
        self._chooser = chooser
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_work(self):
        self._logger.debug(
            f'Begin get_work from {self._config.working_directory} in '
            f'{self.__class__.__name__}.'
        )
        file_list = os.listdir(self._config.working_directory)
        work = deque()
        for f in file_list:
            f_name = None
            if f.endswith('.fits') or f.endswith('.fits.gz'):
                if self._chooser is None:
                    f_name = f
                else:
                    if self._chooser.use_compressed(f):
                        if f.endswith('.fits'):
                            f_name = f'{f}.gz'
                        else:
                            f_name = f
                    else:
                        if f.endswith('.fits.gz'):
                            f_name = f.replace('.gz', '')
                        else:
                            f_name = f
            elif f.endswith('.header'):
                f_name = f
            elif f.endswith('.fz'):
                f_name = f
            elif f.endswith('.hdf5') or f.endswith('.h5'):
                f_name = f
            elif f.endswith('.json'):
                f_name = f
            elif f.endswith('.cat'):
                # NGVS
                f_name = f
            elif f.endswith('.mask.rd.reg'):
                # NGVS
                f_name = f
            if f_name is not None:
                self._logger.debug(f'{f_name} added to work list.')
                work.append(f_name)
        # ensure unique entries
        temp = deque(set(work))
        self._logger.debug(f'End get_work in {self.__class__.__name__}.')
        return temp


class ListDirSeparateDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing for a collection of directories.

    This specialization is meant to imitate the behaviour of the original
    "ListDirDataSource", with different assumptions about which directories
    to use as a source of files, and how to identify files of interest.
    """

    def __init__(self, config):
        super().__init__(config)
        self._source_directories = config.data_sources
        self._extensions = config.data_source_extensions
        self._recursive = config.recurse_data_sources
        self._work = deque()
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        for source in self._source_directories:
            self._logger.info(f'Look in {source} for work.')
            self._append_work(source)
        self._logger.debug('End get_work')
        return self._work

    def _append_work(self, entry):
        with os.scandir(entry) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    self._append_work(entry.path)
                else:
                    for extension in self._extensions:
                        if entry.name.endswith(extension):
                            self._logger.debug(
                                f'Adding {entry.path} to work list.'
                            )
                            self._work.append(entry.path)
                            break


class ListDirTimeBoxDataSource(DataSource):
    """
    A time-boxed directory listing of all .fits* and .hdf5 files. The time-box
    is based on os.stat.st_mtime for a file.

    Implementation choices:
    - one or many directories? one is fire-and-forget, many is probably
      more accurate of operational conditions - e.g. VLASS1.1, VLASS1.2,
      etc, OR vos:Nat/NIFS/...., OR NEOSSAT's NESS, 2017, 2018, 2019, etc
    - send it off to glob, walk, or do it semi-here? one/a few high-level
      directories that point to a lot of occupied storage space could take a
      very long time, and a lot of memory, in glob/walk
    """

    def __init__(self, config):
        """

        :param config: manage_composable.Config
        """
        super().__init__(config)
        self._source_directories = config.data_sources
        self._extensions = config.data_source_extensions
        self._recursive = config.recurse_data_sources
        self._work = deque()
        self._temp = defaultdict(list)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        :param prev_exec_time datetime start of the timestamp chunk
        :param exec_time datetime end of the timestamp chunk
        :return: a deque of StateRunnerMeta instances, with
            prev_exec_time <= os.stat.mtime <= exec_time, and sorted by
            os.stat.mtime
        """
        self._logger.debug(
            f'Begin get_time_box_work from {prev_exec_time} to {exec_time}.'
        )
        for source in self._source_directories:
            self._logger.debug(f'Looking for work in {source}')
            self._append_work(prev_exec_time, exec_time, source)
        # ensure the result returned is sorted by timestamp in ascending
        # order
        for mtime in sorted(self._temp):
            for entry in self._temp[mtime]:
                self._work.append(
                    StateRunnerMeta(entry_name=entry, entry_ts=mtime)
                )
        self._logger.debug('End get_time_box_work')
        self._temp = defaultdict(list)
        return self._work

    def _append_work(self, prev_exec_time, exec_time, entry_path):
        with os.scandir(entry_path) as dir_listing:
            for entry in dir_listing:
                # the slowest thing to do is the 'stat' call, so delay it as
                # long as possible, and only if necessary
                if entry.is_dir() and self._recursive:
                    entry_stats = entry.stat()
                    if exec_time >= entry_stats.st_mtime >= prev_exec_time:
                        self._append_work(
                            prev_exec_time, exec_time, entry.path
                        )
                else:
                    # send the dir_listing value
                    if self.default_filter(entry):
                        entry_stats = entry.stat()
                        if (
                            exec_time
                            >= entry_stats.st_mtime
                            >= prev_exec_time
                        ):
                            self._temp[entry_stats.st_mtime].append(
                                entry.path
                            )


class LocalFilesDataSource(ListDirTimeBoxDataSource):
    """
    For when use_local_files: True and cleanup_when_storing: True
    """

    def __init__(self, config, cadc_client, metadata_reader, recursive=True, scheme='cadc'):
        super().__init__(config)
        self._retry_failures = config.retry_failures
        self._retry_count = config.retry_count
        self._cadc_client = cadc_client
        self._cleanup_when_storing = config.cleanup_files_when_storing
        self._cleanup_failure_directory = config.cleanup_failure_destination
        self._cleanup_success_directory = config.cleanup_success_destination
        self._store_modified_files_only = config.store_modified_files_only
        self._source_directories = config.data_sources
        self._collection = config.collection
        self._recursive = recursive
        self._metadata_reader = metadata_reader
        self._logger = logging.getLogger(self.__class__.__name__)
        self._is_connected = config.is_connected
        self._scheme = scheme
        if not self._is_connected:
            # assume iterative testing is the objective for SCRAPE'ing,
            # and over-ride the configuration that will undermine that
            # behaviour.
            self._cleanup_when_storing = False
            self._logger.info(
                'SCRAPE\'ing data - over-riding config.yml '
                'cleanup_files_when_storing setting.'
            )
        if mc.TaskType.STORE not in config.task_types:
            # do not clean up files unless the STORE task is configured
            self._cleanup_when_storing = False
            self._logger.info(
                'Not STORE\'ing data - ignore config.yml '
                'cleanup_files_when_storing setting.'
            )

    def get_collection(self, ignore=None):
        return self._collection

    def clean_up(self, entry, execution_result, current_count=0):
        """
        Move a file to the success or failure location, depending on
        whether a file with the same checksum is at CADC.

        Move the file only after all retries (as specified in config.yml)
        have been attempted. Note that this cleanup behaviour will be assigned
        to the TodoFileDataSource during a retry.

        :param entry: either a data_source_composable.StateRunnerMeta instance
            or an str, depending on whether the clean-up is invoked from a
            time-boxed or all-in-one invocation.
        :param execution_result: int if it's 0, it's ok to clean up,
            regardless of how many times a file has been processed
        :param current_count: int how many retries have been executed
        """
        self._logger.debug(f'Begin clean_up with {entry}')
        if self._cleanup_when_storing and (
            (not self._retry_failures)
            or (self._retry_failures and current_count >= self._retry_count)
            or (self._retry_failures and execution_result == 0)
        ):
            if isinstance(entry, str):
                fqn = entry
            else:
                fqn = entry.entry_name
            self._logger.debug(f'Clean up {fqn}')
            if self._check_md5sum(fqn):
                # the transfer itself failed, so track as a failure
                self._move_action(fqn, self._cleanup_failure_directory)
            else:
                self._move_action(fqn, self._cleanup_success_directory)
        self._logger.debug('End clean_up.')

    def _check_file(self, fqn):
        """
        Check file content for correctness, by whatever rules the file needs to conform to. The default
        implementation is for FITS files only.

        :param fqn: str fully-qualified file name
        :return: True if the file passes the check, False otherwise
        """
        return ac.check_fitsverify(fqn)

    def default_filter(self, entry):
        """
        :param entry: os.DirEntry
        """
        work_with_file = True
        if super().default_filter(entry):
            if entry.name.startswith('.'):
                # skip dot files
                work_with_file = False
            elif '.hdf5' in entry.name:
                # no hdf5 validation
                pass
            elif self._check_file(entry.path):
                # only work with files that pass the FITS verification
                if self._cleanup_when_storing:
                    if self._store_modified_files_only:
                        # only transfer files with a different MD5 checksum
                        work_with_file = self._check_md5sum(entry.path)
                        if not work_with_file:
                            self._logger.warning(
                                f'{entry.path} has the same md5sum at CADC. '
                                f'Not transferring.'
                            )
                            # KW - 23-06-21
                            # if the file already exists, with the same
                            # checksum, at CADC, Kanoa says move it to the
                            # 'succeeded' directory.
                            self._move_action(
                                entry.path, self._cleanup_success_directory
                            )
                            if self._capture_success is not None:
                                self._capture_success(entry.name, entry.name, datetime.utcnow().timestamp())
                else:
                    work_with_file = True
            else:
                if self._cleanup_when_storing:
                    self._logger.warning(
                        f'Moving {entry.path} to '
                        f'{self._cleanup_failure_directory}'
                    )
                    self._move_action(
                        entry.path, self._cleanup_failure_directory
                    )
                if self._capture_failure is not None:
                    temp_storage_name = mc.StorageName(file_name=entry.name)
                    self._capture_failure(temp_storage_name, BaseException('_check_file errors'), '_check_file errors')
                work_with_file = False
        else:
            work_with_file = False
        self._logger.debug(
            f'Done default_filter says work_with_file is '
            f'{work_with_file} for {entry}'
        )
        return work_with_file

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        for source in self._source_directories:
            self._logger.info(f'Look in {source} for work.')
            self._find_work(source)
        self._logger.debug('End get_work')
        return self._work

    def _append_work(self, prev_exec_time, exec_time, entry_path):
        with os.scandir(entry_path) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    entry_stats = entry.stat()
                    if exec_time >= entry_stats.st_mtime >= prev_exec_time:
                        self._append_work(
                            prev_exec_time, exec_time, entry.path
                        )
                else:
                    # order the stats check before the default_filter check,
                    # because CFHT likes to work with tens of thousands of
                    # files, not a few, and the default filter is the one
                    # that opens and reads every file to see if it's a valid
                    # FITS file
                    #
                    # send the dir_listing value
                    # skip dot files, but have a special exclusion, because
                    # otherwise the entry.stat() call will sometimes fail.
                    if not entry.name.startswith('.'):
                        entry_stats = entry.stat()
                        if (
                            exec_time
                            >= entry_stats.st_mtime
                            >= prev_exec_time
                        ):
                            if self.default_filter(entry):
                                self._temp[entry_stats.st_mtime].append(
                                    entry.path
                                )

    def _check_md5sum(self, entry_path):
        """
        :return: boolean False if the metadata is the same locally as at
            CADC, True otherwise
        """
        # get the metadata locally
        result = True
        if self._is_connected:
            # get the CADC FileInfo
            f_name = os.path.basename(entry_path)
            destination_name = mc.build_uri(self.get_collection(f_name), f_name, self._scheme)
            try:
                cadc_meta = self._cadc_client.info(destination_name)
            except Exception as e:
                self._logger.error(
                    f'info call failed for {destination_name} with {e}'
                )
                self._logger.debug(traceback.format_exc())
                cadc_meta = None

            if cadc_meta is None:
                result = True
            else:
                # get the local FileInfo
                temp_storage_name = mc.StorageName(
                    file_name=f_name, source_names=[entry_path]
                )
                temp_storage_name._destination_uris = [destination_name]
                self._metadata_reader.set_file_info(temp_storage_name)

                if (
                    self._metadata_reader.file_info.get(destination_name).md5sum.replace('md5:', '')
                    == cadc_meta.md5sum.replace('md5:', '')
                ):
                    result = False
        else:
            self._logger.debug(
                f'SCRAPE\'ing data - no md5sum checking with CADC for '
                f'{entry_path}.'
            )
        temp_text = 'different' if result else 'same'
        self._logger.debug(
            f'Done _check_md5sum for {entry_path} result is {temp_text} at '
            f'CADC.'
        )
        return result

    def _find_work(self, entry_path):
        with os.scandir(entry_path) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    self._find_work(entry.path)
                else:
                    if self.default_filter(entry):
                        self._logger.info(
                            f'Adding {entry.path} to work list.'
                        )
                        self._work.append(entry.path)

    def _move_action(self, fqn, destination):
        # if move when storing is enabled, move to an after-action location
        if self._cleanup_when_storing:
            # shutil.move is atomic if it's within a file system, which I
            # believe is the description Kanoa gave. It also supports
            # the same behaviour as
            # https://www.gnu.org/software/coreutils/manual/html_node/
            # mv-invocation.html#mv-invocation when copying between
            # file systems for moving a single file.
            try:
                f_name = os.path.basename(fqn)
                # if the destination is a fully-qualified name, an
                # over-write will succeed
                dest_fqn = os.path.join(destination, f_name)
                self._logger.debug(f'Moving {fqn} to {dest_fqn}')
                shutil.move(fqn, dest_fqn)
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                self._logger.error(f'Failed to move {fqn} to {destination}')
                raise mc.CadcException(e)


class TodoFileDataSource(DataSource):
    """
    Implements the identification of the work to be done, by reading the
    contents of a file.
    """

    def __init__(self, config):
        super().__init__(config)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_work(self):
        self._logger.debug(
            f'Begin get_work from {self._config.work_fqn} in '
            f'{self.__class__.__name__}'
        )
        work = deque()
        with open(self._config.work_fqn) as f:
            for line in f:
                temp = line.strip()
                if len(temp) > 0:
                    # ignore empty lines
                    self._logger.debug(f'Adding entry {temp} to work list.')
                    work.append(temp)
        self._logger.debug(f'End get_work in {self.__class__.__name__}')
        return work


def is_offset_aware(dt):
    """
    Raises CadcException if tzinfo is not set
    :param dt:
    :return: a datetime.timestamp with tzinfo set
    """
    if dt.tzinfo is None:
        raise mc.CadcException(f'Expect tzinfo to be set for {dt}')
    return dt


@dataclass
class StateRunnerMeta:
    # how to refer to the item of work to be processed
    entry_name: str
    # offset-aware timestamp associated with item of work
    entry_ts: datetime.timestamp  # = field(default_factory=is_offset_aware)


class QueryTimeBoxDataSource(DataSource):
    """
    Implements the identification of the work to be done, by querying a
    TAP service, in time-boxed chunks. The time values are timestamps
    (floats).

    Deprecate the QueryTimeBoxDataSource class in favour of this
    implementation.
    """

    def __init__(self, config, preview_suffix='jpg'):
        super().__init__(config)
        self._preview_suffix = preview_suffix
        subject = clc.define_subject(config)
        self._client = CadcTapClient(subject, resource_id=self._config.tap_id)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        Get a set of file names from a collection. Limit the entries by
        time-boxing on lastModified, and don't include previews.

        :param prev_exec_time timestamp start of the time-boxed chunk
        :param exec_time timestamp end of the time-boxed chunk
        :return: a list of StateRunnerMeta instances in the CADC storage system
        """
        # SG 8-09-22
        # All timestamps in the SI databases are in UT
        #
        # SGo - the Docker images all run at UTC, so just use the timestamps as retrieved/stored in state.yml file.
        # Use 'lastModified', because that should be the later timestamp (avoid eventual consistency lags).
        self._logger.debug(f'Begin get_time_box_work.')
        db_fmt = '%Y-%m-%d %H:%M:%S.%f'
        prev_exec_time_utc = datetime.strftime(datetime.utcfromtimestamp(prev_exec_time), db_fmt)
        exec_time_utc = datetime.strftime(datetime.utcfromtimestamp(exec_time), db_fmt)
        query = f"""
            SELECT A.uri, A.lastModified
            FROM inventory.Artifact AS A
            WHERE A.uri NOT LIKE '%{self._preview_suffix}'
            AND A.lastModified > '{prev_exec_time_utc}'
            AND A.lastModified <= '{exec_time_utc}'
            AND split_part( split_part( A.uri, '/', 1 ), ':', 2 ) = '{self._config.collection}'
            ORDER BY A.lastModified ASC
        """
        self._logger.debug(query)
        rows = clc.query_tap_client(query, self._client)
        result = deque()
        for row in rows:
            ignore_scheme, ignore_path, f_name = mc.decompose_uri(row['uri'])
            result.append(StateRunnerMeta(f_name, row['lastModified']))
        self._logger.debug(f'End get_time_box_work.')
        return result


class RenameURIDataSource(DataSource):
    """
    Implement a Data Source that queries for CAOM2 records that have a
    URI that references a compressed file.
    """

    def __init__(self, config, collection, scheme, suffix='.gz'):
        super().__init__(config)
        self._suffix = suffix
        subject = clc.define_subject(config)
        # the resource_id values are hard-coded - if the referenced instances
        # of the services aren't available, the query answers are just
        # considered not available.
        self._argus_client = CadcTapClient(
            subject, resource_id='ivo://cadc.nrc.ca/argus'
        )
        self._collection = collection
        self._scheme = scheme
        self._logger = logging.getLogger(self.__class__.__name__)

    def _query(self):
        # a TAP query to find all the observationID values in CAOM2 with a
        # particular extension
        qs = f"""
        SELECT O.observationID
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P ON O.obsID = P.obsID
        JOIN caom2.Artifact AS A ON A.planeID = P.planeID
        WHERE O.collection = '{self._collection}'
        AND A.uri LIKE f'{self._scheme}:{self._collection}/%.fits{self._suffix}
        """
        return clc.query_tap_client(qs, self._argus_client).to_pandas()

    def get_work(self):
        # make a list for ray, skip the first entry, which is the text header
        # string 'observationID'
        return self._query().observationID.values.tolist()[1:]


class VaultDataSource(ListDirTimeBoxDataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing on VOSpace.

    The directory listing may be time-boxed.
    """

    def __init__(self, vault_client, config):
        super().__init__(config)
        self._vault_client = vault_client
        self._source_directories = config.data_sources
        self._data_source_extensions = config.data_source_extensions
        self._logger = logging.getLogger(__name__)

    def get_work(self):
        self._logger.debug('Begin get_work.')
        work = deque()
        for source_directory in self._source_directories:
            self._logger.debug(
                f'Searching {source_directory} for work to do.'
            )
            self._find_work(source_directory, work)
        self._logger.debug('End get_work.')
        return work

    def _append_work(self, prev_exec_time, exec_time, entry):
        self._logger.info(f'Search for work in {entry}.')
        # force = True means do not use the cache
        node = self._vault_client.get_node(entry, limit=None, force=True)
        while node.type == 'vos:LinkNode':
            uri = node.target
            node = self._vault_client.get_node(uri, limit=None, force=True)
        for target in node.node_list:
            target_node = self._vault_client.get_node(target)
            target_node_mtime = mc.make_time_tz(target_node.props.get('date'))
            if target_node.type == 'vos:ContainerNode' and self._recursive:
                if exec_time >= target_node_mtime >= prev_exec_time:
                    self._append_work(
                        prev_exec_time, exec_time, target_node.uri
                    )
            else:
                if self.default_filter(target_node):
                    if exec_time >= target_node_mtime >= prev_exec_time:
                        self._temp[target_node_mtime].append(target_node.uri)
                        self._logger.info(
                            f'Add {target_node.uri} to work list.'
                        )

    def _find_work(self, source_directory, work):
        node = self._vault_client.get_node(
            source_directory, limit=None, force=True
        )
        while node.type == 'vos:LinkNode':
            uri = node.target
            node = self._vault_client.get_node(uri, limit=None, force=True)
        for entry in node.node_list:
            if entry.type == 'vos:ContainerNode' and self._recursive:
                self._find_work(entry.uri, work)
            else:
                if self.default_filter(entry):
                    self._logger.info(f'Add {entry.uri} to work list.')
                    work.append(entry.uri)

    def default_filter(self, target_node):
        """
        :param target_node: Node
        """
        # make a Node look like an os.DirEntry, which is expected by
        # the super invocation
        dir_entry = type('', (), {})
        dir_entry.name = os.path.basename(target_node.uri)
        dir_entry.path = target_node.uri
        if super().default_filter(dir_entry):
            target_node_size = target_node.props.get('size')
            if target_node_size == 0:
                self._logger.info(f'Skipping 0-length {target_node.uri}')
            else:
                return True
        return False


class VaultCleanupDataSource(VaultDataSource):
    """
    This implementation will clean up a vos: data source, but it will
    not retry it.

    This implementation does not yet rely on a MetadataReader for caching
    the FileInfo.
    """

    def __init__(self, config, vault_client, cadc_client, scheme='cadc'):
        super().__init__(vault_client, config)
        self._cleanup_when_storing = config.cleanup_files_when_storing
        self._cleanup_failure_directory = config.cleanup_failure_destination
        self._cleanup_success_directory = config.cleanup_success_destination
        self._store_modified_files_only = config.store_modified_files_only
        self._collection = config.collection
        self._recursive = config.recurse_data_sources
        self._cadc_client = cadc_client
        self._scheme = scheme
        self._work = deque()
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_collection(self, f_name):
        return self._collection

    def clean_up(self, entry, execution_result, current_count):
        """
        Move a file to the success or failure location, depending on whether
        a file with the same checksum is at CADC.
        """
        if self._cleanup_when_storing:
            self._logger.debug(f'Begin clean_up with {entry}')
            if isinstance(entry, str):
                fqn = entry
            else:
                fqn = entry.entry_name
            self._logger.debug(f'Clean up {fqn}')
            check_result, vos_meta = self._check_md5sum(fqn)
            if check_result:
                # if vos_meta is None, it's already been cleaned up,
                # due to astropy fits verify failure cleanup
                if vos_meta is not None:
                    # the transfer itself failed, so track as a failure
                    self._move_action(fqn, self._cleanup_failure_directory)
            else:
                self._move_action(fqn, self._cleanup_success_directory)
            self._logger.debug('End clean_up.')

    def default_filter(self, entry, entry_fqn):
        copy_file = False
        for extension in self._data_source_extensions:
            if entry.endswith(extension):
                if entry.startswith('.'):
                    # skip dot files
                    copy_file = False
                elif self._store_modified_files_only:
                    # only transfer files with a different MD5 checksum
                    copy_file, ignore_meta = self._check_md5sum(entry_fqn)
                    if not copy_file and self._cleanup_when_storing:
                        self._move_action(
                            entry_fqn, self._cleanup_success_directory
                        )
                        if self._capture_success is not None:
                            self._capture_success(entry.name, entry.name, datetime.utcnow().timestamp())
                else:
                    copy_file = True
                break
        return copy_file

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        for source in self._source_directories:
            self._logger.info(f'Look in {source} for work.')
            self._find_work(source)
        self._logger.debug('End get_work')
        return self._work

    def _check_md5sum(self, entry_fqn):
        # get the metadata from VOS
        result = True
        vos_meta = clc.vault_info(self._vault_client, entry_fqn)
        # get the metadata at CADC
        f_name = os.path.basename(entry_fqn)
        collection = self.get_collection(f_name)
        cadc_name = mc.build_uri(collection, f_name, self._scheme)
        cadc_meta = self._cadc_client.info(cadc_name)
        if cadc_meta is not None and vos_meta.md5sum == cadc_meta.md5sum:
            self._logger.warning(
                f'{entry_fqn} has the same md5sum at CADC. Not transferring.'
            )
            result = False
        return result, vos_meta

    def _find_work(self, entry):
        dir_listing = self._vault_client.listdir(entry)
        for dir_entry in dir_listing:
            dir_entry_fqn = f'{entry}/{dir_entry}'
            if self._vault_client.isdir(dir_entry_fqn) and self._recursive:
                self._find_work(dir_entry_fqn)
            else:
                if self.default_filter(dir_entry, dir_entry_fqn):
                    self._logger.info(f'Adding {dir_entry_fqn} to work list.')
                    self._work.append(dir_entry_fqn)

    def _move_action(self, fqn, destination):
        """
        :param fqn: VOS URI, includes a file name
        :param destination: VOS URI, points to a directory
        :return:
        """
        # if move when storing is enabled, move to an after-action location
        if self._cleanup_when_storing:
            try:
                f_name = os.path.basename(fqn)
                dest_fqn = os.path.join(destination, f_name)
                try:
                    if self._vault_client.status(dest_fqn):
                        # vos: doesn't support over-write
                        self._logger.warning(
                            f'Removing {dest_fqn} prior to over-write.'
                        )
                        self._vault_client.delete(dest_fqn)
                except exceptions.NotFoundException as not_found_e:
                    # do thing, since the node doesn't exist
                    pass
                self._logger.warning(f'Moving {fqn} to {dest_fqn}')
                self._vault_client.move(fqn, dest_fqn)
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                self._logger.error(f'Failed to move {fqn} to {destination}')
                raise mc.CadcException(e)


def data_source_factory(config, clients, state):
    """
    :param config: manage_composable.Config
    :param clients: client_composable.ClientCollection
    :param state: bool True is the DataSource is time-boxed.
    :return:
    """
    if config.use_local_files:
        source = ListDirSeparateDataSource(config)
    else:
        if config.use_vos and clients.vo_client is not None:
            if config.cleanup_files_when_storing:
                source = VaultCleanupDataSource(
                    config, clients.vo_client, clients.data_client
                )
            else:
                source = VaultDataSource(clients.vo_client, config)
        else:
            if state:
                source = QueryTimeBoxDataSource(config)
            else:
                source = TodoFileDataSource(config)
    logging.debug(f'Created {type(source)} data_source_composable instance.')
    return source

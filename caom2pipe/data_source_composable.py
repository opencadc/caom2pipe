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

import logging
import os

from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime
from dateutil import tz

from cadctap import CadcTapClient
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc

__all__ = [
    'DataSource',
    'ListDirDataSource',
    'ListDirSeparateDataSource',
    'ListDirTimeBoxDataSource',
    'QueryTimeBoxDataSource',
    'QueryTimeBoxDataSourceTS',
    'StateRunnerMeta',
    'TodoFileDataSource',
    'VaultListDirDataSource',
]


class DataSource(object):
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
        list of work todo as a method implementation
    """

    def __init__(self, config=None):
        self._config = config
        # if this value is used, it should be a timestamp - i.e. a float
        self._start_time_ts = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def clean_up(self):
        pass

    def get_work(self):
        return []

    def get_time_box_work(self, prev_exec_time, exec_time):
        return []

    @property
    def start_time_ts(self):
        return self._start_time_ts

    @start_time_ts.setter
    def start_time_ts(self, value):
        self._start_time_ts = value


class ListDirDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing.
    """

    def __init__(self, config, chooser):
        super(ListDirDataSource, self).__init__(config)
        self._chooser = chooser
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_work(self):
        self._logger.debug(
            f'Begin get_work from {self._config.working_directory} in '
            f'{self.__class__.__name__}.'
        )
        file_list = os.listdir(self._config.working_directory)
        work = []
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
        temp = list(set(work))
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

    def __init__(self, config, recursive=True):
        super(ListDirSeparateDataSource, self).__init__(config)
        self._source_directories = config.data_sources
        self._extensions = config.data_source_extensions
        self._recursive = recursive
        self._work = []
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

    def __init__(self, config, recursive=True):
        """

        :param config: manage_composable.Config
        :param recursive: True if sub-directories should also be checked
        """
        super(ListDirTimeBoxDataSource, self).__init__(config)
        self._source_directories = config.data_sources
        self._extensions = config.data_source_extensions
        self._recursive = recursive
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

    def _append_work(self, prev_exec_time, exec_time, entry):
        with os.scandir(entry) as dir_listing:
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

    def default_filter(self, entry):
        for extension in self._extensions:
            if entry.name.endswith(extension):
                return True
        return False


class TodoFileDataSource(DataSource):
    """
    Implements the identification of the work to be done, by reading the
    contents of a file.
    """

    def __init__(self, config):
        super(TodoFileDataSource, self).__init__(config)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_work(self):
        self._logger.debug(
            f'Begin get_work from {self._config.work_fqn} in '
            f'{self.__class__.__name__}'
        )
        work = []
        with open(self._config.work_fqn) as f:
            for line in f:
                temp = line.strip()
                if len(temp) > 0:
                    # ignore empty lines
                    logging.debug(f'Adding entry {temp} to work list.')
                    work.append(temp)
        self._logger.debug(f'End get_work in {self.__class__.__name__}')
        return work


class QueryTimeBoxDataSource(DataSource):
    """
    Implements the identification of the work to be done, by querying a
    TAP service, in time-boxed chunks.
    """

    def __init__(self, config, preview_suffix='jpg'):
        super(QueryTimeBoxDataSource, self).__init__(config)
        self._preview_suffix = preview_suffix
        subject = clc.define_subject(config)
        self._client = CadcTapClient(subject, resource_id=self._config.tap_id)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        Get a set of file names from an archive. Limit the entries by
        time-boxing on ingestDate, and don't include previews.

        :param prev_exec_time datetime start of the timestamp chunk
        :param exec_time datetime end of the timestamp chunk
        :return: a list of file names in the CADC storage system
        """
        # container timezone is UTC, ad timezone is Pacific
        db_fmt = '%Y-%m-%d %H:%M:%S.%f'
        prev_exec_time_pz = datetime.strftime(
            prev_exec_time.astimezone(tz.gettz('US/Pacific')), db_fmt
        )
        exec_time_pz = datetime.strftime(
            exec_time.astimezone(tz.gettz('US/Pacific')), db_fmt
        )
        self._logger.debug(f'Begin get_work.')
        query = (
            f"SELECT fileName, ingestDate FROM archive_files WHERE "
            f"archiveName = '{self._config.archive}' "
            f"AND fileName not like '%{self._preview_suffix}' "
            f"AND ingestDate > '{prev_exec_time_pz}' "
            f"AND ingestDate <= '{exec_time_pz}' "
            "ORDER BY ingestDate ASC "
        )
        self._logger.debug(query)
        result = deque()
        rows = clc.query_tap_client(query, self._client)
        for row in rows:
            result.append(StateRunnerMeta(row['fileName'], row['ingestDate']))
        return result


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


class QueryTimeBoxDataSourceTS(DataSource):
    """
    Implements the identification of the work to be done, by querying a
    TAP service, in time-boxed chunks. The time values are timestamps
    (floats).

    Deprecate the QueryTimeBoxDataSource class in favour of this
    implementation.
    """

    def __init__(self, config, preview_suffix='jpg'):
        super(QueryTimeBoxDataSourceTS, self).__init__(config)
        self._preview_suffix = preview_suffix
        subject = clc.define_subject(config)
        self._client = CadcTapClient(subject, resource_id=self._config.tap_id)
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        Get a set of file names from an archive. Limit the entries by
        time-boxing on ingestDate, and don't include previews.

        :param prev_exec_time timestamp start of the time-boxed chunk
        :param exec_time timestamp end of the time-boxed chunk
        :return: a list of StateRunnerMeta instances in the CADC storage
            system
        """
        # container timezone is UTC, ad timezone is Pacific
        db_fmt = '%Y-%m-%d %H:%M:%S.%f'
        prev_exec_time_pz = datetime.strftime(
            datetime.utcfromtimestamp(prev_exec_time).astimezone(
                tz.gettz('US/Pacific')
            ),
            db_fmt,
        )
        exec_time_pz = datetime.strftime(
            datetime.utcfromtimestamp(exec_time).astimezone(
                tz.gettz('US/Pacific')
            ),
            db_fmt,
        )
        self._logger.debug(f'Begin get_work.')
        query = (
            f"SELECT fileName, ingestDate FROM archive_files WHERE "
            f"archiveName = '{self._config.archive}' "
            f"AND fileName NOT LIKE '%{self._preview_suffix}' "
            f"AND ingestDate > '{prev_exec_time_pz}' "
            f"AND ingestDate <= '{exec_time_pz}' "
            "ORDER BY ingestDate ASC "
        )
        self._logger.debug(query)
        rows = clc.query_tap_client(query, self._client)
        result = deque()
        for row in rows:
            result.append(StateRunnerMeta(row['fileName'], row['ingestDate']))
        return result


class VaultListDirDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing.
    """

    def __init__(self, vos_client, config):
        super(VaultListDirDataSource, self).__init__(config)
        self._client = vos_client
        self._source_directories = config.data_sources
        self._data_source_extensions = config.data_source_extensions
        self._logger = logging.getLogger(__name__)

    def get_work(self):
        self._logger.debug('Begin get_work.')
        work = []
        for source_directory in self._source_directories:
            file_list = self._client.listdir(source_directory)
            for f_name in file_list:
                for ending in self._data_source_extensions:
                    if f_name.endswith(ending):
                        fqn = f'{source_directory}/{f_name}'
                        work.append(fqn)
                        self._logger.debug(f'{fqn} added to work list.')
                        break
        # ensure unique entries
        temp = list(set(work))
        self._logger.debug('End get_work.')
        return temp

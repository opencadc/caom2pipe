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

from cadctap import CadcTapClient
from caom2pipe import manage_composable as mc

__all__ = ['DataSource']


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
    - an implementation of the work_composable.work class, which returns a
        list of work todo as a method implementation
    """

    def __init__(self, config):
        self._config = config
        self._logger = logging.getLogger(__name__)

    def get_work(self):
        return []

    def get_time_box_work(self, prev_exec_time, exec_time):
        return []


class ListDirDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing.
    """

    def __init__(self, config, chooser):
        super(ListDirDataSource, self).__init__(config)
        self._chooser = chooser
        self._logger = logging.getLogger(__name__)

    def get_work(self):
        self._logger.debug(f'Begin get_work from '
                           f'{self._config.working_directory} in '
                           f'{self.__class__.__name__}.')
        file_list = os.listdir(self._config.working_directory)
        work = []
        for f in file_list:
            f_name = None
            if f.endswith('.fits') or f.endswith('.fits.gz'):
                if (self._chooser is not None and
                        self._chooser.use_compressed(f)):
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
            if f_name is not None:
                self._logger.debug(f'{f_name} added to work list.')
                work.append(f_name)
        # ensure unique entries
        temp = list(set(work))
        self._logger.debug(f'End get_work in {self.__class__.__name__}.')
        return temp


class TodoFileDataSource(DataSource):
    """
    Implements the identification of the work to be done, by reading the
    contents of a file.
    """

    def __init__(self, config):
        super(TodoFileDataSource, self).__init__(config)
        self._logger = logging.getLogger(__name__)

    def get_work(self):
        self._logger.debug(f'Begin get_work from {self._config.work_fqn} in '
                           f'{self.__class__.__name__}')
        work = []
        with open(self._config.work_fqn) as f:
            for line in f:
                logging.debug(f'Adding entry {line.strip()} to work list.')
                work.append(line.strip())
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
        subject = mc.define_subject(config)
        self._client = CadcTapClient(subject, resource_id=self._config.tap_id)
        self._logger = logging.getLogger(__name__)

    def get_time_box_work(self, prev_exec_time, exec_time):
        """
        Get a set of file names from an archive. Limit the entries by
        time-boxing on ingestDate, and don't include previews.

        :param prev_exec_time datetime start of the timestamp chunk
        :param exec_time datetime end of the timestamp chunk
        :return: a list of file names in the CADC storage system
        """
        self._logger.debug(f'Begin get_work in {self.__class__.__name__}.')
        query = f"SELECT fileName, ingestDate FROM archive_files WHERE " \
                f"archiveName = '{self._config.archive}' " \
                f"AND fileName not like '%{self._preview_suffix}' " \
                f"AND ingestDate > '{prev_exec_time}' " \
                f"AND ingestDate <= '{exec_time}' " \
                "ORDER BY ingestDate ASC "
        self._logger.debug(query)
        return mc.query_tap_client(query, self._client)

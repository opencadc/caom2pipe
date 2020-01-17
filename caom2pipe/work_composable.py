# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

from cadctap import CadcTapClient
from caom2pipe import manage_composable as mc


__all__ = ['Work', 'StorageTimeBoxQuery']


class Work(object):
    """"Abstract-like class that defines the operations used to chunk work when
    controlling execution by State."""

    def __init__(self, max_ts_s):
        """
        Ctor
        :param max_ts_s: the maximum timestamp in seconds, for work that
        is chunked by time-boxes.
        """
        self._max_ts_s = max_ts_s
        self._logger = logging.getLogger(__name__)

    @property
    def max_ts_s(self):
        # State execution is currently chunked only by time-boxing, so
        # set the maximum time-box ending for the work to be done.
        return self._max_ts_s

    def initialize(self):
        """Anything necessary to make todo work."""
        raise NotImplementedError

    def todo(self, prev_exec_date, exec_date):
        """Returns a list of entries for processing by Execute.
        :param prev_exec_date when chunking by time-boxes, the start time
        :param exec_date when chunking by time-boxes, the end time"""
        raise NotImplementedError


class StorageTimeBoxQuery(Work):

    def __init__(self, max_ts, config, preview_suffix='jpg'):
        super(StorageTimeBoxQuery, self).__init__(max_ts.timestamp())
        self._config = config
        self.max_ts = max_ts  # type datetime
        self._preview_suffix = preview_suffix
        subject = mc.define_subject(config)
        self._client = CadcTapClient(subject, resource_id=self._config.tap_id)

    def todo(self, prev_exec_date, exec_date):
        """
        Get a set of file names from an archive. Limit the entries by
        time-boxing on ingestDate.

        :param prev_exec_date datetime start of the timestamp chunk
        :param exec_date datetime end of the timestamp chunk
        :return: a list of file names in the CADC storage system
        """
        self._logger.debug('Entering StorageTimeBoxQuery.todo')
        query = f"SELECT fileName, ingestDate FROM archive_files WHERE " \
                f"archiveName = '{self._config.archive}' " \
                f"AND fileName not like '%{self._preview_suffix}' " \
                f"AND ingestDate > '{prev_exec_date}' " \
                f"AND ingestDate <= '{exec_date}' " \
                "ORDER BY ingestDate ASC "
        self._logger.debug(query)
        return mc.query_tap_client(query, self._client, self._config.tap_id)

    def initialize(self):
        """Do nothing."""
        pass

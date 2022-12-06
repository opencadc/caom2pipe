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
import logging
import os

import pandas as pd
from datetime import timezone

from cadcutils.net import Subject
from cadctap import CadcTapClient
from caom2pipe.manage_composable import Config, query_tap_pandas, write_as_yaml
from caom2pipe.run_composable import set_logging

__all__ = ['Validator', 'VALIDATE_OUTPUT']

VALIDATE_OUTPUT = 'validated.yml'


class Validator:
    """
    Compares the state of CAOM entries at CADC with the state of the source
    of the data. Identifies files that are in CAOM entries that do not exist
    at the source, and files at the source that are not represented in CAOM.
    Checks that the timestamp associated with the file at the source is less
    than the ad timestamp.

    CADC is the destination, where the data and metadata originate from
    is the source.

    The method 'read_from_source' must be implemented for validate to
    run to completion.

    ##########     a          ##########
    #        #     ->         #        #
    # SOURCE #                #  META  #
    #        #     <-         #        #
    ##########     b          ##########

       ^   \                  e  ^  |  f
        \   \                    |  v
         \   \
    c     \   \    d          ##########
           \   \              #        #
            \   - - >         #  DATA  #
             -  -             #        #
                              ##########
    """

    def __init__(
        self,
        source_name,
        scheme='cadc',
        preview_suffix='jpg',
        source_tz=timezone.utc,
    ):
        """
        :param source_name: String value used for logging
        :param scheme: string which encapsulates scheme as used in CAOM Artifact URIs.
        :param preview_suffix String value that is excluded from queries, usually for files that are produced at CADC.
        :param source_tz String representation of timezone name, as understood by timezone.
        """
        self._config = Config()
        self._config.get_executors()
        set_logging(self._config)
        subject = Subject(certificate=self._config.proxy_fqn)
        self._caom_client = CadcTapClient(subject=subject, resource_id=self._config.tap_id)
        self._data_client = CadcTapClient(subject=subject, resource_id=self._config.storage_inventory_tap_resource_id)
        self._source = []
        self._destination_data = []
        self._destination_meta = []
        self._source_name = source_name
        self._scheme = scheme
        self._preview_suffix = preview_suffix
        self._source_tz = source_tz
        self._logger = logging.getLogger(self.__class__.__name__)

    def _filter_result(self, dest_meta_temp):
        """The default implementation does nothing, but this allows
        implementations that extend this class to remove entries from
        the comparison results for whatever reason might come up."""
        pass

    def _find_unaligned_dates(self, source, data):
        newer = pd.DataFrame()
        if len(data) > 0:
            # SG - 08-09-22 - All timestamps in the SI databases are in UT.
            #
            # source_temp => url, timestamp, f_name
            # data => uri, contentLastModified
            data['f_name'] = data.uri.apply(Validator.filter_meta)
            data['ts'] = data.contentLastModified.apply(pd.to_datetime)
            merged = pd.merge(source, data, how='inner', on=['f_name'])
            newer = merged[merged.timestamp > merged.ts]
        return newer

    def _read_list_from_destination_data(self):
        """Code to execute a query for files and the arrival time, that are in
        CADC storage.
        """
        query = (
            f"SELECT A.uri, A.contentLastModified "
            f"FROM inventory.Artifact AS A "
            f"WHERE A.uri LIKE '%:{self._config.collection}/%' "
            f"AND A.uri NOT LIKE '%{self._preview_suffix}'"
        )
        self._logger.debug(f'Query is {query}')
        temp = query_tap_pandas(query, self._data_client)
        temp['f_name'] = temp.uri.apply(Validator.filter_meta)
        # columns are uri, contentLastModified, f_name
        return temp

    def _read_list_from_destination_meta(self):
        query = (
            f"SELECT O.observationID, A.uri FROM caom2.Observation AS O "
            f"JOIN caom2.Plane AS P ON O.obsID = P.obsID "
            f"JOIN caom2.Artifact AS A ON P.planeID = A.planeID "
            f"WHERE O.collection='{self._config.collection}' "
            f"AND A.uri not like '%{self._preview_suffix}'"
        )
        self._logger.debug(f'Query is {query}')
        temp = query_tap_pandas(query, self._caom_client)
        temp['f_name'] = temp.uri.apply(Validator.filter_meta)
        # columns are observationID, uri, f_name
        return temp

    def read_from_source(self):
        """Read the entire source site listing. This function is expected to
        return a dict of all the file names available from the source, where
        the keys are the file names, and the values are the timestamps at the
        source."""
        raise NotImplementedError()

    def validate(self):
        # META
        self._logger.info('Query destination metadata.')
        dest_meta_temp = self._read_list_from_destination_meta()

        # SOURCE
        self._logger.info('Query source metadata.')
        source_temp = self.read_from_source()

        # SOURCE vs META
        self._logger.info('Find files that do not appear at CADC.')
        # a => files at SOURCE that aren't in META
        self._destination_meta = Validator.find_missing(dest_meta_temp, source_temp, 'f_name')
        self._logger.info(f'Find files that do not appear at {self._source_name}.')
        # b => files in META that aren't at SOURCE
        self._source = Validator.find_missing(source_temp, dest_meta_temp, 'f_name')

        # DATA
        self._logger.info('Query destination data.')
        dest_data_temp = self._read_list_from_destination_data()

        self._logger.info(f'Find files that are newer at {self._source_name} than at CADC.')
        # d' => files newer at SOURCE than in DATA
        self._destination_data = self._find_unaligned_dates(source_temp, dest_data_temp)

        self._logger.info(f'Filter the results.')
        self._filter_result(dest_meta_temp)

        self._logger.info('Log the results.')
        result = {
            f'{self._source_name}': self._source,
            'cadc': self._destination_meta,
            'timestamps': self._destination_data,
        }
        result_fqn = os.path.join(
            self._config.working_directory, VALIDATE_OUTPUT
        )
        write_as_yaml(result, result_fqn)

        self._logger.info(
            f'Results:\n'
            f'  - {len(self._source)} files at {self._source_name} that are '
            f'not referenced by CADC CAOM entries\n'
            f'  - {len(self._destination_meta)} CAOM entries at CADC that do '
            f'not reference {self._source_name} files\n'
            f'  - {len(self._destination_data)} files that are newer at '
            f'{self._source_name} than in CADC storage'
        )
        return self._source, self._destination_meta, self._destination_data

    def write_todo(self):
        """Write a todo.txt file, given the list of entries available from
        the source, that are not currently at the destination (CADC)."""
        raise NotImplementedError()

    @staticmethod
    def filter_meta(meta):
        # meta is usually a URI, and want just a file name
        return meta.split('/')[-1]

    @staticmethod
    def find_missing(in_here, from_there, column):
        return in_here[~in_here[column].isin(from_there[column])]

    @staticmethod
    def make_timestamp(a):
        # return pd.to_datetime(a).timestamp()
        # return pd.to_datetime64(a)
        return pd.to_datetime(a)



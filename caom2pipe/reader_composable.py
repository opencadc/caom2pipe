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

import logging
import tempfile
import traceback

from caom2utils import data_util
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc


__all__ = [
    'FileMetadataReader',
    'MetadataReader',
    'reader_factory',
    'StorageClientReader',
    'VaultReader',
]


class MetadataReader:
    """Wrap the mechanism for retrieving metadata that is used to create a
    CAOM2 record, and to make decisions about how to create that record. Use
    cases are:
        - FITS files on local disk
        - CADC storage client
        - Gemini http client
        - VOSpace client

    Users of this class hierarchy should be able to reduce the number of
    times file headers and FileInfo are retrieved for the same file.

    Use the source for determining FileInfo information because comparing
    the md5sum at the source to CADC storage is how to determine whether or
    not a file needs to be pushed to CADC for storage, should storing files be
    part of the execution.

    TODO - how to handle thumbnails and previews
    """

    def __init__(self):
        # dicts are indexed by mc.StorageName.destination_uris
        self._headers = {}  # astropy.io.fits.Headers
        self._file_info = {}  # cadcdata.FileInfo
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def file_info(self):
        return self._file_info

    @property
    def headers(self):
        return self._headers

    def _retrieve_file_info(self, source_name):
        return None

    def _retrieve_headers(self, source_name):
        return None

    def set(self, storage_name):
        """Retrieves the Header and FileInfo information to memory."""
        self.set_headers(storage_name)
        self.set_file_info(storage_name)

    def set_file_info(self, storage_name):
        """Retrieves FileInfo information to memory."""
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._file_info.keys():
                self._logger.debug(f'Retrieve FileInfo for {entry}')
                self._file_info[entry] = self._retrieve_file_info(
                    storage_name.source_names[index]
                )

    def set_headers(self, storage_name):
        """Retrieves the Header information to memory."""
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._headers.keys():
                if '.fits' in entry:
                    self._logger.debug(f'Retrieve headers for {entry}')
                    self._headers[entry] = (
                        self._retrieve_headers(
                            storage_name.source_names[index]
                        )
                    )
                else:
                    self._headers[entry] = []

    def reset(self):
        self._headers = {}
        self._file_info = {}


class FileMetadataReader(MetadataReader):
    """Use case: FITS files on local disk."""

    def __init__(self):
        super().__init__()

    def _retrieve_file_info(self, source_name):
        return data_util.get_local_file_info(source_name)

    def _retrieve_headers(self, source_name):
        return data_util.get_local_headers_from_fits(source_name)


class StorageClientReader(MetadataReader):
    """Use case: CADC storage.

    The storage_name.destination_uris are always the CADC storage reference,
    so if this specialization is being used, rely on that naming,
    instead of using the source names, which is the default implementation.
    """

    def __init__(self, client):
        """

        :param client: StorageClientWrapper instance
        """
        super().__init__()
        self._client = client

    def set_file_info(self, storage_name):
        """Retrieves FileInfo information to memory."""
        for entry in storage_name.destination_uris:
            if entry not in self._file_info.keys():
                self._file_info[entry] = self._client.info(entry)

    def set_headers(self, storage_name):
        """Retrieves the Header information to memory."""
        for entry in storage_name.destination_uris:
            if entry not in self._headers.keys():
                if '.fits' in entry:
                    self._headers[entry] = self._client.get_head(entry)
                else:
                    self._headers[entry] = []


class VaultReader(MetadataReader):
    """Use case: vault."""

    def __init__(self, client):
        """

        :param client: vos.Client instance
        """
        super().__init__()
        self._client = client
        self._logger = logging.getLogger(self.__class__.__name__)

    def _retrieve_file_info(self, source_name):
        return clc.vault_info(self._client, source_name)

    def _retrieve_headers(self, source_name):
        try:
            tmp_file = tempfile.NamedTemporaryFile()
            self._client.copy(source_name, tmp_file.name, head=True)
            temp_header = data_util.get_local_file_headers(tmp_file.name)
            tmp_file.close()
            return temp_header
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            raise mc.CadcException(
                f'Did not retrieve {source_name} header because {e}'
            )


def reader_factory(config, clients):
    metadata_reader = None
    if config.use_local_files or mc.TaskType.SCRAPE in config.task_types:
        metadata_reader = FileMetadataReader()
    else:
        if config.use_vos and clients.vo_client is not None:
            metadata_reader = VaultReader(clients.vo_client)
    if metadata_reader is None:
        metadata_reader = StorageClientReader(clients.data_client)
    return metadata_reader

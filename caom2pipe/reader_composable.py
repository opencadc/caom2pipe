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

from caom2utils import data_util

__all__ = ['MetadataReader']


class MetadataReader:
    """Wrap the mechanism for retrieving metadata that is used to create a
    CAOM2 record, and to make decisions about how to create that record. Use
    cases are:
        - FITS files on local disk
        - CADC storage client
        - Gemini http client
        - VOSpace client

    TODO - how to handle thumbnails and previews
    """

    def __init__(self):
        self._headers = {}  # astropy.io.fits.Headers
        self._file_info = {}  # cadcdata.FileInfo

    def get_file_info(self, uri):
        return self._file_info.get(uri)

    def get_headers(self, uri):
        return self._headers.get(uri)

    def set(self, storage_name):
        """Retrieves the Header and FileInfo information."""

    def reset(self):
        self._headers = {}
        self._file_info = {}


class FileMetadataReader(MetadataReader):
    """Use case: FITS files on local disk."""

    def __init__(self):
        super().__init__()

    def set(self, storage_name):
        for index, entry in enumerate(storage_name.destination_uris):
            if '.fits' in entry:
                self._headers[entry] = (
                    data_util.get_local_headers_from_fits(
                        storage_name.source_names[index]
                    )
                )
            else:
                self._headers[entry] = []
            self._file_info[entry] = data_util.get_local_file_info(
                storage_name.source_names[index]
            )


class StorageClientReader(MetadataReader):
    """Use case: CADC storage."""

    def __init__(self, client):
        """

        :param client: StorageClientWrapper instance
        """
        super().__init__()
        self._client = client

    def set(self, storage_name):
        for index, entry in enumerate(storage_name.destination_uris):
            if '.fits' in entry:
                self._headers[entry] = (
                    self._client.get_head(storage_name.source_names[index])
                )
            else:
                self._headers[entry] = []
            self._file_info[entry] = self._client.info(
                storage_name.source_names[index]
            )

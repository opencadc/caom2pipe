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

"""
This module contains classes and functions that have to do with managing
and using long-term connections to CADC services.

Leave out:
- get_artifact_metadata_client, since it mostly CAOM2 dependent, and the
  only client-based part is get_cadc_meta_client, which is already here
- should Metrics be here? Maybe.
"""

import logging
import os
import traceback

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO, StringIO
from sys import getsizeof

from astropy.table import Table

from cadcdata import CadcDataClient
from cadctap import CadcTapClient
from cadcutils import net, exceptions
from caom2pipe import manage_composable as mc
from caom2repo import CAOM2RepoClient
from vos import Client

__all__ = [
    'client_get',
    'client_put',
    'client_put_fqn',
    'ClientCollection',
    'current',
    'data_get',
    'data_put',
    'data_put_fqn',
    'define_subject',
    'get_cadc_headers_client',
    'get_cadc_meta_client',
    'get_cadc_meta_client_v',
    'query_tap_client',
    'repo_create',
    'repo_delete',
    'repo_get',
    'repo_update',
]


class ClientCollection(object):
    """
    This class initializes and provides accessors to known HTTP clients
    for CADC services.

    This is time-dependent cohesiveness. The clients should all be declared
    at approximately the same time, will live as long as the pipeline
    instance, and require the same AAI credential information, even though
    there is one per not-related-at-all CADC service.

    Eventually session retries, etc, might be configurable, but that need has
    not yet been demonstrated.
    """

    def __init__(self, config):
        self._metadata_client = None
        self._data_client = None
        self._query_client = None
        self._metrics = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._init(config)

    @property
    def data_client(self):
        return self._data_client

    @property
    def metadata_client(self):
        return self._metadata_client

    @property
    def metrics(self):
        return self._metrics

    @property
    def query_client(self):
        return self._query_client

    def _init(self, config):
        subject = mc.define_subject(config)

        if mc.TaskType.SCRAPE in config.task_types:
            self._logger.info(
                f'SCRAPE\'ing data - no clients will be initialized.'
            )
        else:
            self._metadata_client = CAOM2RepoClient(
                subject, config.logging_level, config.resource_id
            )

            if config.features.supports_latest_client:
                self._logger.warning('Using vos.Client for storage.')
                cert_file = config.proxy_fqn
                if cert_file is not None and os.path.exists(cert_file):
                    self._data_client = Client(vospace_certfile=cert_file)
                else:
                    raise mc.CadcException(
                        'No credentials configured or found. Stopping.'
                    )
            else:
                self._logger.warning(
                    'Using cadcdata.CadcDataClient for storage.'
                )
                self._data_client = CadcDataClient(subject)

            if config.tap_id is not None:
                self._query_client = CadcTapClient(
                    subject=subject, resource_id=config.tap_id
                )
        self._metrics = mc.Metrics(config)


def client_get(client, working_directory, file_name, source, metrics):
    """
    Retrieve a local copy of a file available from CADC. Assumes the working
    directory location exists and is writeable.

    :param client: The Client for read access to CADC storage.
    :param working_directory: Where 'file_name' will be written.
    :param file_name: What to copy from CADC storage.
    :param source: Where to retrieve the file from.
    :param metrics: track success execution times, and failure counts.
    """
    start = current()
    fqn = os.path.join(working_directory, file_name)
    try:
        retrieved_size = client.copy(source, destination=fqn)
        if not os.path.exists(fqn):
            raise mc.CadcException(f'Retrieve failed. {fqn} does not exist.')
        file_size = os.stat(fqn).st_size
        if retrieved_size != file_size:
            raise mc.CadcException(
                f'Wrong file size {retrieved_size} retrieved for {source}.'
            )
    except Exception as e:
        metrics.observe_failure('copy', 'vos', file_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Did not retrieve {fqn} because {e}')
    end = current()
    metrics.observe(start, end, file_size, 'copy', 'vos', file_name)


def client_put(
    client, working_directory, file_name, storage_name, metrics=None
):
    """
    Make a copy of a locally available file by writing it to CADC. Assumes
    file and directory locations are correct.

    Will check that the size of the file stored is the same as the size of
    the file on disk.

    :param client: Client for write access to CADC storage.
    :param working_directory: Where 'file_name' exists locally.
    :param file_name: What to copy to CADC storage.
    :param storage_name: Where to write the file.
    :param metrics: Tracking success execution times, and failure counts.
    """
    start = current()
    try:
        fqn = os.path.join(working_directory, file_name)
        stored_size = client.copy(fqn, destination=storage_name)
        file_size = os.stat(fqn).st_size
        if stored_size != file_size:
            raise mc.CadcException(
                f'Stored file size {stored_size} != {file_size} at CADC for '
                f'{storage_name}.'
            )
    except Exception as e:
        metrics.observe_failure('copy', 'vos', file_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Failed to store data with {e}')
    end = current()
    metrics.observe(start, end, file_size, 'copy', 'vos', file_name)


def client_put_fqn(client, source_name, destination_name, metrics=None):
    """
    Make a copy of a locally available file by writing it to CADC. Assumes
    file and directory locations are correct.

    Will check that the size of the file stored is the same as the size of
    the file on disk.

    :param client: Client for write access to CADC storage.
    :param source_name: fully-qualified file name on local storage
    :param destination_name: Where to write the file.
    :param metrics: Tracking success execution times, and failure counts.
    """
    start = current()
    try:
        stored_size = client.copy(source_name, destination=destination_name)
        file_size = os.stat(source_name).st_size
        if stored_size != file_size:
            raise mc.CadcException(
                f'Stored file size {stored_size} != {file_size} at CADC for '
                f'{destination_name}.'
            )
    except Exception as e:
        metrics.observe_failure('copy', 'vos', os.path.basename(source_name))
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Failed to store data with {e}')
    end = current()
    metrics.observe(
        start, end, file_size, 'copy', 'vos', os.path.basename(source_name)
    )


def current():
    """Encapsulate returning UTC now in microsecond resolution."""
    return datetime.utcnow().timestamp()


def data_get(client, working_directory, file_name, archive, metrics):
    """
    Retrieve a local copy of a file available from CADC. Assumes the working
    directory location exists and is writeable.

    :param client: The CadcDataClient for read access to CADC storage.
    :param working_directory: Where 'file_name' will be written.
    :param file_name: What to copy from CADC storage.
    :param archive: Which archive to retrieve the file from.
    :param metrics: track success execution times, and failure counts.
    """
    start = current()
    fqn = os.path.join(working_directory, file_name)
    try:
        client.get_file(archive, file_name, destination=fqn)
        if not os.path.exists(fqn):
            raise mc.CadcException(
                f'ad retrieve failed. {fqn} does not exist.'
            )
    except Exception as e:
        metrics.observe_failure('get', 'data', file_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Did not retrieve {fqn} because {e}')
    end = current()
    file_size = os.stat(fqn).st_size
    metrics.observe(start, end, file_size, 'get', 'data', file_name)


def data_put(
    client,
    working_directory,
    file_name,
    archive,
    stream='raw',
    mime_type=None,
    mime_encoding=None,
    metrics=None,
):
    """
    Make a copy of a locally available file by writing it to CADC. Assumes
    file and directory locations are correct. Requires a checksum comparison
    by the client.

    :param client: The CadcDataClient for write access to CADC storage.
    :param working_directory: Where 'file_name' exists locally.
    :param file_name: What to copy to CADC storage.
    :param archive: Which archive to associate the file with.
    :param stream: Defaults to raw - use is deprecated, however necessary it
        may be at the current moment to the 'put_file' call.
    :param mime_type: Because libmagic can't see inside a zipped fits file.
    :param mime_encoding: Also because libmagic can't see inside a zipped
        fits file.
    :param metrics: Tracking success execution times, and failure counts.
    """
    start = current()
    cwd = os.getcwd()
    try:
        os.chdir(working_directory)
        client.put_file(
            archive,
            file_name,
            archive_stream=stream,
            mime_type=mime_type,
            mime_encoding=mime_encoding,
            md5_check=True,
        )
        file_size = os.stat(file_name).st_size
    except Exception as e:
        metrics.observe_failure('put', 'data', file_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Failed to store data with {e}')
    finally:
        os.chdir(cwd)
    end = current()
    metrics.observe(start, end, file_size, 'put', 'data', file_name)


def data_put_fqn(
    client,
    source_name,
    storage_name,
    stream='raw',
    metrics=None,
):
    """
    Make a copy of a locally available file by writing it to CADC. Assumes
    file and directory locations are correct. Requires a checksum comparison
    by the client.

    :param client: The CadcDataClient for write access to CADC storage.
    :param source_name: str fully-qualified
    :param storage_name: StorageName instance
    :param stream: str A relic of the old CADC storage.
    :param metrics: Tracking success execution times, and failure counts.
    """
    start = current()
    try:
        client.put_file(
            storage_name.archive,
            source_name,
            archive_stream=stream,
            mime_type=storage_name.mime_type,
            mime_encoding=storage_name.mime_encoding,
            md5_check=True,
        )
        file_size = os.stat(source_name).st_size
    except Exception as e:
        metrics.observe_failure('put', 'data', source_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Failed to store data with {e}')
    end = current()
    metrics.observe(start, end, file_size, 'put', 'data', source_name)


def define_subject(config):
    """Common code to figure out which credentials to use based on the
    content of a Config instance."""
    subject = None
    if config.proxy_fqn is not None and os.path.exists(config.proxy_fqn):
        logging.debug(
            f'Using proxy certificate {config.proxy_fqn} for credentials.'
        )
        subject = net.Subject(username=None, certificate=config.proxy_fqn)
    elif config.netrc_file is not None:
        netrc_fqn = os.path.join(config.working_directory, config.netrc_file)
        if os.path.exists(netrc_fqn):
            logging.debug(f'Using netrc file {netrc_fqn} for credentials.')
            subject = net.Subject(
                username=None, certificate=None, netrc=netrc_fqn
            )
        else:
            logging.warning(f'Cannot find netrc file {netrc_fqn}')
    else:
        logging.warning(
            f'Proxy certificate is {config.proxy_fqn}, netrc file is '
            f'{config.netrc_file}.'
        )
        raise mc.CadcException(
            'No credentials provided (proxy certificate or netrc file). '
            'Cannot create an anonymous subject.'
        )
    return subject


def get_cadc_headers_client(archive, file_name, client):
    """
    Creates the FITS headers object by fetching the FITS headers of a CADC
    file. The function takes advantage of the fhead feature of the CADC
    storage service and retrieves just the headers and no data, minimizing
    the transfer time.

    The file may be public or proprietary, depending on the capabilities of
    the supplied client parameter.

    :param archive: CADC Archive reference, as a string
    :param file_name: CADC Archive file name, as a string. Includes compression
        and file type extensions.
    :param client: CadcDataClient instance.
    :return: a string of keyword/value pairs.
    """
    b = BytesIO()
    b.name = file_name
    client.get_file(archive, file_name, b, fhead=True)
    fits_header = b.getvalue().decode('ascii')
    b.close()
    return fits_header


def get_cadc_meta_client(client, archive, fname):
    """
    Gets contentType, contentLength and contentChecksum of a CADC artifact
    :param client: CadcDataClient instance
    :param archive: archive file has been stored to
    :param fname: name of file in the archive
    :return:
    """
    return client.get_file_info(archive, fname)


@dataclass
class FileMeta:
    """The bits of information about a file that are used to decide whether
    or not to transfer, and whether or not a transfer was successful."""
    f_size: int
    md5sum: str


def get_cadc_meta_client_v(storage_name, cadc_client):
    """
    Retrieve metadata for a single file. This implementation uses a
    vos.Client.

    This is a very bad implementation, but there is no information on what
    might be a better one.

    :param storage_name: Artifact URI
    :param cadc_client:
    :return:
    """
    node = cadc_client.get_node(storage_name, limit=None, force=False)
    f_size = node.props.get('length')
    f_md5sum = node.props.get('MD5')
    return FileMeta(f_size, f_md5sum)


def query_tap_client(query_string, tap_client):
    """
    :param query_string ADQL
    :param tap_client which client to query the service with
    :returns an astropy votable instance."""

    logging.debug(f'query_tap_client: execute query \n{query_string}')
    buffer = StringIO()
    tap_client.query(
        query_string,
        output_file=buffer,
        data_only=True,
        response_format='csv',
    )
    return Table.read(buffer.getvalue().split('\n'), format='csv')


def repo_create(client, observation, metrics):
    start = current()
    try:
        client.create(observation)
    except Exception as e:
        metrics.observe_failure('create', 'caom2', observation.observation_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not create an observation record for '
            f'{observation.observation_id}. {e}'
        )
    end = current()
    metrics.observe(
        start,
        end,
        getsizeof(observation),
        'create',
        'caom2',
        observation.observation_id,
    )


def repo_delete(client, collection, obs_id, metrics):
    start = current()
    try:
        client.delete(collection, obs_id)
    except Exception as e:
        metrics.observe_failure('delete', 'caom2', obs_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not delete the observation record for {obs_id}. {e}'
        )
    end = current()
    metrics.observe(start, end, 0, 'delete', 'caom2', obs_id)


def repo_get(client, collection, obs_id, metrics):
    start = current()
    try:
        observation = client.read(collection, obs_id)
    except exceptions.NotFoundException:
        observation = None
    except Exception:
        metrics.observe_failure('read', 'caom2', obs_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not retrieve an observation record for {obs_id}.'
        )
    end = current()
    metrics.observe(
        start, end, getsizeof(observation), 'read', 'caom2', obs_id
    )
    return observation


def repo_update(client, observation, metrics):
    start = current()
    try:
        client.update(observation)
    except Exception as e:
        metrics.observe_failure('update', 'caom2', observation.observation_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not update an observation record for '
            f'{observation.observation_id}. {e}'
        )
    end = current()
    metrics.observe(
        start,
        end,
        getsizeof(observation),
        'update',
        'caom2',
        observation.observation_id
    )


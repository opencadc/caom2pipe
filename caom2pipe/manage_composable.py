# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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

import csv
import importlib
import io
import logging
import os
import re
import stat

import requests
import subprocess
import sys
import traceback
import yaml

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from dateutil import tz
from enum import Enum
from hashlib import md5
from importlib_metadata import version
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib import parse as parse
from urllib3 import Retry

from astropy.table import Table

from cadcutils import net
from cadcdata import CadcDataClient
from cadctap import CadcTapClient
from caom2 import ObservationWriter, ObservationReader, Artifact, Observation
from caom2 import ChecksumURI, ProductType, ReleaseType
from caom2.diff import get_differences


__all__ = [
    'build_uri',
    'Cache',
    'CadcException',
    'CaomName',
    'compare_observations',
    'Config',
    'check_param',
    'convert_to_days',
    'convert_to_ts',
    'decompose_lineage',
    'exec_cmd',
    'exec_cmd_info',
    'exec_cmd_redirect',
    'extract_file_name_from_uri',
    'Features',
    'FileMeta',
    'ftp_get',
    'ftp_get_timeout',
    'get_artifact_metadata',
    'get_cadc_headers',
    'get_cadc_meta',
    'get_endpoint_session',
    'get_file_meta',
    'get_keyword',
    'get_lineage',
    'http_get',
    'increment_time',
    'increment_time_tz',
    'ISO_8601_FORMAT',
    'load_module',
    'make_seconds',
    'make_time',
    'make_time_tz',
    'Metrics',
    'minimize_on_keyword',
    'Observable',
    'query_endpoint',
    'query_endpoint_session',
    'read_csv_file',
    'read_file_list_from_archive',
    'read_from_file',
    'read_obs_from_file',
    'Rejected',
    'reverse_lookup',
    'StorageName',
    'State',
    'TaskType',
    'to_float',
    'to_int',
    'to_str',
    'update_typed_set',
    'VALIDATE_OUTPUT',
    'Validator',
    'ValueRepairCache',
    'write_obs_to_file',
    'write_to_file',
]

ISO_8601_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
READ_BLOCK_SIZE = 8 * 1024
VALIDATE_OUTPUT = 'validated.yml'


class CadcException(Exception):
    """Generic exception raised by failure cases within the caom2pipe
    module."""

    pass


class Features:
    """Boolean feature flag implementation."""

    def __init__(self):
        self._run_in_airflow = True
        self._supports_composite = True
        self._supports_catalog = True
        self._supports_latest_client = False
        self._supports_multiple_files = True
        self._expects_retry = True

    @property
    def run_in_airflow(self):
        """If true, will treat command-line arguments as if the application
        is running in airflow."""
        return self._run_in_airflow

    @run_in_airflow.setter
    def run_in_airflow(self, value):
        self._run_in_airflow = value

    @property
    def supports_composite(self):
        """If true, will execute any specific code for composite observation
        definition."""
        return self._supports_composite

    @supports_composite.setter
    def supports_composite(self, value):
        self._supports_composite = value

    @property
    def supports_catalog(self):
        """If true, will execute any specific code for catalog handling
        when creating a CAOM instance."""
        return self._supports_catalog

    @supports_catalog.setter
    def supports_catalog(self, value):
        self._supports_catalog = value

    @property
    def supports_latest_client(self):
        """If true, will execute any latest-version-specific code when creating
        a CAOM instance."""
        return self._supports_latest_client

    @supports_latest_client.setter
    def supports_latest_client(self, value):
        self._supports_latest_client = value

    @property
    def supports_multiple_files(self):
        """If true, will execute any specific code where the cardinality
        between metadata and files is 1:n."""
        return self._supports_multiple_files

    @supports_multiple_files.setter
    def supports_multiple_files(self, value):
        self._supports_multiple_files = value

    @property
    def expects_retry(self):
        """If true, will execute any specific code for running retries
        based on retries_log.txt content."""
        return self._expects_retry

    @expects_retry.setter
    def expects_retry(self, value):
        self._expects_retry = value

    def __str__(self):
        return ' '.join(
            f'{ii} {getattr(self, ii)}' for ii in vars(self)
        )


class TaskType(Enum):
    """The possible steps in a Collection pipeline. A short-hand, user-facing
    way to identify the work to be done by a pipeline."""

    STORE = 'store'  # store a file to CADC
    SCRAPE = 'scrape'  # create/update a CAOM instance with no network
    INGEST = 'ingest'  # create/update a CAOM instance from metadata only
    MODIFY = 'modify'  # data access observation visitors
    VISIT = 'visit'  # metadata access observation visitors
    # update a CAOM instance using CAOM metadata as input and knowledge
    INGEST_OBS = 'ingest_obs'


class State:
    """Persist information between pipeline invocations.

    Currently the State class persists the concept of a bookmark, which is the
    place in the flow of data that was last processed. This 'place' may be a
    timestamp, or an id. That value is up to clients of this class.
    """

    def __init__(self, fqn):
        self.fqn = fqn
        self.bookmarks = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        result = read_as_yaml(self.fqn)
        if result is None:
            raise CadcException(f'Could not load state from {fqn}')
        else:
            self.bookmarks = result.get('bookmarks')
            self.context = result.get('context', {})
            self.content = result

    def get_bookmark(self, key):
        """Lookup for last_record key. Treat like an offset-aware datetime."""
        result = None
        if key in self.bookmarks:
            result = self.bookmarks.get(key).get('last_record')
            if result:
                result = make_time_tz(result)
        else:
            self.logger.warning(f'No record found for {key}')
        return result

    def get_context(self, key):
        result = None
        if key in self.context:
            result = self.context.get(key)
        else:
            self.logger.warning(f'No context found for {key}')
        return result

    def save_state(self, key, value):
        """Write the current state as a YAML file.
        :param key which record is being updated
        :param value the value to update the record with
        """
        bookmarks = self.get_bookmark(key)
        if bookmarks is None:
            context = self.get_context(key)
            if context is None:
                self.logger.warning(f'No content found for {key}')
            else:
                self.context[key] = value
                logging.debug(f'Saving context {value} {self.fqn}')
                write_as_yaml(self.content, self.fqn)
        else:
            self.bookmarks[key]['last_record'] = value
            logging.debug(f'Saving bookmarked last record {value} {self.fqn}')
            write_as_yaml(self.content, self.fqn)


class Rejected:
    """Persist information between pipeline invocations about the observation
    IDs that will fail a particular TaskType.
    """

    NO_REASON = ''
    BAD_DATA = 'bad_data'
    BAD_METADATA = 'bad_metadata'
    INVALID_FORMAT = 'is_valid_fails'
    MISSING = 'missing_at_source'
    NO_INSTRUMENT = 'no_instrument'
    NO_PREVIEW = 'no_preview'

    # A map to the logging message string representing acknowledged rejections
    reasons = {
        BAD_DATA: 'Header missing END card',
        BAD_METADATA: 'Cannot build an observation',
        INVALID_FORMAT: 'Invalid observation ID',
        MISSING: 'Could not find JSON record for',
        NO_INSTRUMENT: 'Unknown value for instrument',
        NO_PREVIEW: 'Internal Server Error for url: '
        'https://archive.gemini.edu/preview',
    }

    def __init__(self, fqn):
        """
        Ctor
        :param fqn: fully-qualified name for reading/writing rejected
        information records. If the file exists, initialize the in-memory
        records with the content of the file. Otherwise initialize the
        in-memory records to be empty.
        """
        self.fqn = fqn
        if os.path.exists(fqn):
            try:
                self.content = read_as_yaml(fqn)
            except yaml.constructor.ConstructorError:
                logging.error(f'ConstructorError reading {self.fqn}')
                self.content = {}
        else:
            dir_name = os.path.dirname(fqn)
            create_dir(dir_name)
            self.content = {}
        for reason in Rejected.reasons.keys():
            if reason not in self.content:
                self.content[reason] = []

    def __str__(self):
        return self.content.__str__()

    def check_and_record(self, message, obs_id):
        """Keep track of an additional entry.
        :returns boolean True if the failure is known and tracked."""
        for reason, reason_str in Rejected.reasons.items():
            if reason_str in message:
                self.record(reason, obs_id)
                return True
        return False

    def record(self, reason, entry):
        """Keep track of an additional entry."""
        self.content[reason].append(entry)

    def persist_state(self):
        """Write the current state as a YAML file."""
        for key, value in self.content.items():
            # ensure unique entries
            self.content[key] = list(set(value))
        write_as_yaml(self.content, self.fqn)

    def get_bad_data(self):
        return self.content[Rejected.BAD_DATA]

    def get_bad_metadata(self):
        return self.content[Rejected.BAD_METADATA]

    def get_no_preview(self):
        return self.content[Rejected.NO_PREVIEW]

    def is_bad_data(self, obs_id):
        return obs_id in self.content[Rejected.BAD_DATA]

    def is_bad_metadata(self, obs_id):
        return obs_id in self.content[Rejected.BAD_METADATA]

    def is_no_preview(self, file_name):
        return file_name in self.content[Rejected.NO_PREVIEW]

    @staticmethod
    def known_failure(message):
        """Returns the REASON for the failure, or an empty string if
        the failure is of an unexpected type.
        """
        result = Rejected.NO_REASON
        for reason, text in Rejected.reasons.items():
            if text in message:
                result = reason
        return result


class Metrics:
    """
    A class to capture execution metrics.
    """

    def __init__(self, config):
        self.enabled = config.observe_execution
        if self.enabled:
            self.history = {}
            self.failures = {}
            self.observable_dir = config.observable_directory

    def observe(self, start, stop, size, action, service, label):
        if self.enabled:
            elapsed = round(stop - start, 3)
            rate = round(size / (stop - start), 3)
            if service not in self.history:
                self.history[service] = {}
            if action not in self.history[service]:
                self.history[service][action] = {}
            self.history[service][action][label] = [elapsed, rate, start]

    def observe_failure(self, action, service, label):
        if self.enabled:
            if service not in self.failures:
                self.failures[service] = {}
            if action not in self.failures[service]:
                self.failures[service][action] = {}
            if label not in self.failures[service][action]:
                self.failures[service][action][label] = 1
            else:
                self.failures[service][action][label] += 1

    def capture(self):
        if self.enabled:
            create_dir(self.observable_dir)
            now = datetime.utcnow().timestamp()
            for service in self.history.keys():
                fqn = os.path.join(self.observable_dir, f'{now}.{service}.yml')
                write_as_yaml(self.history[service], fqn)

            fqn = os.path.join(self.observable_dir, f'{now}.fail.yml')
            write_as_yaml(self.failures, fqn)


def minimize_on_keyword(x, candidate):
    result = x
    if candidate is not None:
        if x is None:
            result = candidate
        else:
            result = min(x, candidate)
    return result


class Observable:
    """
    A class to contain all the classes that maintain information between
    pipeline execution instances.
    """

    def __init__(self, rejected, metrics):
        self._rejected = rejected
        self._metrics = metrics

    @property
    def rejected(self):
        return self._rejected

    @property
    def metrics(self):
        return self._metrics


class Cache:
    """Abstract-like class to store persistent information that originates
    from outside of a pipeline invocation.
    """

    def __init__(self, rigorous_get=True):
        """ """
        config = Config()
        config.get_executors()
        self._fqn = config.cache_fqn
        # if True, raise exceptions, which tends to call a halt to any
        # pipeline. If False, only log a warning.
        self._rigorous_get = rigorous_get
        self._logger = logging.getLogger(self.__class__.__name__)
        try:
            self._cache = read_as_yaml(self._fqn)
        except Exception as e:
            raise CadcException(
                f'Cache file {self._fqn} read failure {e}. Stopping pipeline.'
            )

    def add_to(self, key, values):
        """Add to or update the content of the cache. This is an over-write
        operation."""
        self._cache[key] = values

    def get_from(self, key):
        result = self._cache.get(key)
        if result is None:
            msg = f'Failed to find key {key} in cache {self._fqn}.'
            if self._rigorous_get:
                raise CadcException(msg)
            else:
                self._logger.warning(msg)
                result = []
        return result

    def save(self):
        """Write the current state as a YAML file."""
        write_as_yaml(self._cache, self._fqn)


class CaomName:
    """The naming rules for making and decomposing CAOM URIs (i.e. Observation
    URIs, Plane URIs, and archive URIs, all isolated in one class. There are
    probably OMM assumptions built in, but those will slowly go away :)."""

    def __init__(self, uri):
        self.uri = uri
        ignore_scheme, ignore_path, file_name = decompose_uri(self.uri)
        self._file_name = file_name
        self._file_id = StorageName.remove_extensions(file_name)

    @property
    def file_id(self):
        """

        :return: Extracted from an Artifact URI, the file_id is the file
        name portion of the URI with all file type and compression type
        extensions removed.
        """
        return self._file_id

    @property
    def file_name(self):
        """:return The file name extracted from an Artifact URI."""
        return self._file_name

    @property
    def uncomp_file_name(self):
        """:return The file name extracted from an Artifact URI, without
        the compression extension."""
        return self._file_name.replace('.gz', '')

    @staticmethod
    def make_obs_uri_from_obs_id(collection, obs_id):
        """:return A string that conforms to the Observation URI
        specification from CAOM."""
        return f'caom:{collection}/{obs_id}'

    @staticmethod
    def decompose_provenance_input(uri):
        # looks like:
        # caom:OMM/C170323_0151_CAL/C170323_0151_CAL
        bits = uri.split('/')
        obs_id = bits[1]
        product_id = bits[2]
        return obs_id, product_id

    @staticmethod
    def extract_file_name(entry):
        """
        :param entry: may be a URL, a fully-qualified file name, or a simple
            file name. Extract the file name from which-ever one of those
            structures it is.
        :return:
        """
        temp = parse.urlparse(entry)
        if temp.scheme == '':
            file_name = os.path.basename(entry)
        else:
            if temp.scheme.startswith('http'):
                file_name = temp.path.split('/')[-1]
            else:
                # it's an Artifact URI
                file_name = temp.path.split('/')[-1]
        return file_name


class Config:
    """Configuration information that remains the same for all steps and all
    work in a pipeline execution."""

    def __init__(self):
        self._working_directory = None
        self._work_file = None
        # the fully qualified name for the work file
        self.work_fqn = None
        self._netrc_file = None
        self._archive = None
        self._collection = None
        self._use_local_files = False
        self._resource_id = None
        self._tap_id = None
        self._logging_level = None
        self._log_to_file = False
        self._log_file_directory = None
        self._stream = None
        self._storage_host = None
        self._task_types = []
        self._success_log_file_name = None
        # the fully qualified name for the file
        self.success_fqn = None
        self._failure_log_file_name = None
        # the fully qualified name for the file
        self.failure_fqn = None
        self._cleanup_files_when_storing = False
        self._report_fqn = None
        self._retry_file_name = None
        # the fully qualified name for the file
        self.retry_fqn = None
        self._retry_failures = False
        self._retry_count = 1
        self._proxy_file_name = None
        # the fully qualified name for the file
        self.proxy_fqn = None
        self._state_file_name = None
        # the fully qualified name for the file
        self.state_fqn = None
        self._rejected_file_name = None
        self._rejected_directory = None
        # the fully qualified name for the file
        self.rejected_fqn = None
        self.slack_channel = None
        self.slack_token = None
        self._store_modified_files_only = False
        self._progress_file_name = None
        self.progress_fqn = None
        self._interval = None
        self._observe_execution = False
        self._observable_directory = None
        self._source_host = None
        self._cache_file_name = None
        # the fully qualified name for the file
        self.cache_fqn = None
        self._data_sources = []
        self._data_source_extensions = ['.fits']
        self._recurse_data_sources = True
        self._features = Features()
        self._cleanup_failure_destination = None
        self._cleanup_success_destination = None
        self._storage_inventory_resource_id = None

    @property
    def is_connected(self):
        return TaskType.SCRAPE not in self._task_types

    @property
    def working_directory(self):
        """the root directory for all executor operations"""
        return self._working_directory

    @working_directory.setter
    def working_directory(self, value):
        self._working_directory = value

    @property
    def work_file(self):
        """the file that contains the list of work to be passed through the
        pipeline"""
        return self._work_file

    @work_file.setter
    def work_file(self, value):
        self._work_file = value
        if self._working_directory is not None:
            self.work_fqn = os.path.join(
                self._working_directory, self._work_file
            )

    @property
    def data_sources(self):
        """Root URI for data retrieval"""
        return self._data_sources

    @data_sources.setter
    def data_sources(self, value):
        self._data_sources = value

    @property
    def data_source_extensions(self):
        """Root URI for data retrieval"""
        return self._data_source_extensions

    @data_source_extensions.setter
    def data_source_extensions(self, value):
        self._data_source_extensions = value

    @property
    def netrc_file(self):
        """credentials for any service calls"""
        return self._netrc_file

    @netrc_file.setter
    def netrc_file(self, value):
        self._netrc_file = value

    @property
    def collection(self):
        """which collection is addressed by the pipeline"""
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    @property
    def archive(self):
        """which archive is addressed by the pipeline"""
        return self._archive

    @archive.setter
    def archive(self, value):
        self._archive = value

    @property
    def use_local_files(self):
        """changes expectations of the executors for handling files on disk"""
        return self._use_local_files

    @use_local_files.setter
    def use_local_files(self, value):
        self._use_local_files = value

    @property
    def resource_id(self):
        """which service instance to use"""
        return self._resource_id

    @resource_id.setter
    def resource_id(self, value):
        self._resource_id = value

    @property
    def tap_id(self):
        """which tap service instance to use"""
        return self._tap_id

    @tap_id.setter
    def tap_id(self, value):
        self._tap_id = value

    @property
    def log_to_file(self):
        """boolean - write the log to a file?"""
        return self._log_to_file

    @log_to_file.setter
    def log_to_file(self, value):
        self._log_to_file = value

    @property
    def log_file_directory(self):
        """where log files are written to - defaults to working_directory"""
        return self._log_file_directory

    @log_file_directory.setter
    def log_file_directory(self, value):
        self._log_file_directory = value

    @property
    def logging_level(self):
        """the logging level - enforced throughout the pipeline"""
        return self._logging_level

    @logging_level.setter
    def logging_level(self, value):
        lookup = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
        }
        if value in lookup:
            self._logging_level = lookup[value]

    @property
    def cleanup_files_when_storing(self):
        """boolean - clean up files when finished with a possibly unsuccessful
        attempt to STORE."""
        return self._cleanup_files_when_storing

    @cleanup_files_when_storing.setter
    def cleanup_files_when_storing(self, value):
        self._cleanup_files_when_storing = value

    @property
    def cleanup_success_destination(self):
        """
        If cleanup_files_when_storing is set to True, after a successful
        STORE to CADC, the file will be 'mv'd to this destination. Probably
        a fully-qualified directory name, but HTTP is always a possibility.
        """
        return self._cleanup_success_destination

    @cleanup_success_destination.setter
    def cleanup_success_destination(self, value):
        self._cleanup_success_destination = value

    @property
    def cleanup_failure_destination(self):
        """
        If cleanup_files_when_storing is set to True, after a failed
        STORE to CADC, the file will be 'mv'd to this destination. Probably
        a fully-qualified directory name, but HTTP is always a possibility.
        """
        return self._cleanup_failure_destination

    @cleanup_failure_destination.setter
    def cleanup_failure_destination(self, value):
        self._cleanup_failure_destination = value

    @property
    def recurse_data_sources(self):
        """If true, will do a recursive search through `data_sources`."""
        return self._recurse_data_sources

    @recurse_data_sources.setter
    def recurse_data_sources(self, value):
        self._recurse_data_sources = value

    @property
    def stream(self):
        """the ad 'stream' that goes with the archive - use when storing
        files"""
        return self._stream

    @stream.setter
    def stream(self, value):
        self._stream = value

    @property
    def storage_host(self):
        """the ad 'host' to store files to - used for testing cadc-data put
        commands only, should usually be None"""
        return self._storage_host

    @storage_host.setter
    def storage_host(self, value):
        self._storage_host = value

    @property
    def storage_inventory_resource_id(self):
        return self._storage_inventory_resource_id

    @storage_inventory_resource_id.setter
    def storage_inventory_resource_id(self, value):
        self._storage_inventory_resource_id = value

    @property
    def task_types(self):
        """the way to control which steps get executed"""
        return self._task_types

    @task_types.setter
    def task_types(self, value):
        self._task_types = value

    @property
    def success_log_file_name(self):
        """the filename where success logs are written, this will be created
        in log_file_directory"""
        return self._success_log_file_name

    @success_log_file_name.setter
    def success_log_file_name(self, value):
        self._success_log_file_name = value
        if self._log_file_directory is not None:
            self.success_fqn = os.path.join(
                self._log_file_directory, self._success_log_file_name
            )

    @property
    def failure_log_file_name(self):
        """the filename where failure logs are written this will be created
        in log_file_directory"""
        return self._failure_log_file_name

    @failure_log_file_name.setter
    def failure_log_file_name(self, value):
        self._failure_log_file_name = value
        if self._log_file_directory is not None:
            self.failure_fqn = os.path.join(
                self._log_file_directory, self._failure_log_file_name
            )

    @property
    def report_fqn(self):
        return self._report_fqn

    @property
    def retry_file_name(self):
        """the filename where retry entries are written this will be created
        in log_file_directory"""
        return self._retry_file_name

    @retry_file_name.setter
    def retry_file_name(self, value):
        self._retry_file_name = value
        if self._log_file_directory is not None:
            self.retry_fqn = os.path.join(
                self._log_file_directory, self._retry_file_name
            )

    @property
    def retry_failures(self):
        """Will the application retry the entries in the
        retries.txt file? If True, the application will attempt to re-run
        the work to do for each entry in the retries.txt file. If False,
        it will do nothing."""
        return self._retry_failures

    @retry_failures.setter
    def retry_failures(self, value):
        self._retry_failures = value

    @property
    def retry_count(self):
        """how many times the application will retry the entries in the
        retries.txt file."""
        return self._retry_count

    @retry_count.setter
    def retry_count(self, value):
        self._retry_count = value

    @property
    def rejected_directory(self):
        """the directory where rejected entry files are written, by default
        will be the log file directory"""
        return self._rejected_directory

    @rejected_directory.setter
    def rejected_directory(self, value):
        self._rejected_directory = value
        if self._rejected_directory is not None:
            self.rejected_fqn = os.path.join(
                self._rejected_directory, self._rejected_file_name
            )
        elif self._log_file_directory is not None:
            self.rejected_fqn = os.path.join(
                self._log_file_directory, self._rejected_file_name
            )

    @property
    def rejected_file_name(self):
        """the filename where rejected entries are written, this will be created
        in log_file_directory"""
        return self._rejected_file_name

    @rejected_file_name.setter
    def rejected_file_name(self, value):
        self._rejected_file_name = value
        if self._rejected_directory is not None:
            self.rejected_fqn = os.path.join(
                self._rejected_directory, self._rejected_file_name
            )
        elif self._log_file_directory is not None:
            self.rejected_fqn = os.path.join(
                self._log_file_directory, self._rejected_file_name
            )

    @property
    def slack_channel(self):
        return self._slack_channel

    @slack_channel.setter
    def slack_channel(self, value):
        self._slack_channel = value

    @property
    def slack_token(self):
        return self._slack_token

    @slack_token.setter
    def slack_token(self, value):
        self._slack_token = value

    @property
    def store_modified_files_only(self):
        return self._store_modified_files_only

    @store_modified_files_only.setter
    def store_modified_files_only(self, value):
        self._store_modified_files_only = value

    @property
    def progress_file_name(self):
        """the filename where pipeline progress is written, this will be created
        in log_file_directory. Useful when using timestamp windows for
        execution."""
        return self._progress_file_name

    @progress_file_name.setter
    def progress_file_name(self, value):
        self._progress_file_name = value
        if self._log_file_directory is not None:
            self.progress_fqn = os.path.join(
                self._log_file_directory, self._progress_file_name
            )

    @property
    def proxy_file_name(self):
        """If using a proxy certificate for authentication, identify the
        fully-qualified pathname here."""
        return self._proxy_file_name

    @proxy_file_name.setter
    def proxy_file_name(self, value):
        self._proxy_file_name = value
        if (
            self._working_directory is not None
            and self._proxy_file_name is not None
        ):
            self.proxy_fqn = os.path.join(
                self._working_directory, self._proxy_file_name
            )

    @property
    def state_file_name(self):
        """If using a state file to communicate persistent information between
        invocations, identify the fully-qualified pathname here."""
        return self._state_file_name

    @state_file_name.setter
    def state_file_name(self, value):
        self._state_file_name = value
        if (
            self._working_directory is not None
            and self._state_file_name is not None
        ):
            self.state_fqn = os.path.join(
                self._working_directory, self._state_file_name
            )

    @property
    def cache_file_name(self):
        """If using a cache file to communicate persistent information between
        invocations, identify the fully-qualified pathname here."""
        return self._cache_file_name

    @cache_file_name.setter
    def cache_file_name(self, value):
        self._cache_file_name = value
        if (
            self._working_directory is not None
            and self._cache_file_name is not None
        ):
            self.cache_fqn = os.path.join(
                self._working_directory, self._cache_file_name
            )

    @property
    def features(self):
        """Feature flag setting access."""
        return self._features

    @features.setter
    def features(self, value):
        self._features = value

    @property
    def interval(self):
        """Interval - used for setting timestamp chunks, for now."""
        return self._interval

    @interval.setter
    def interval(self, value):
        self._interval = value

    @property
    def observe_execution(self):
        """If true, time and track the CADC service invocations."""
        return self._observe_execution

    @observe_execution.setter
    def observe_execution(self, value):
        self._observe_execution = value

    @property
    def observable_directory(self):
        """Directory where to track CADC service invocation information."""
        return self._observable_directory

    @observable_directory.setter
    def observable_directory(self, value):
        self._observable_directory = value

    @property
    def source_host(self):
        """Host that is the source of something. Initial use case as ftp
        host name."""
        return self._source_host

    @source_host.setter
    def source_host(self, value):
        self._source_host = value

    def __str__(self):
        return (
            f'\nFrom {os.getcwd()}/config.yml:\n'
            f'  archive:: {self.archive}\n'
            f'  cache_fqn:: {self.cache_fqn}\n'
            f'  cleanup_failure_destination:: '
            f'{self.cleanup_failure_destination}\n'
            f'  cleanup_files_when_storing:: '
            f'{self.cleanup_files_when_storing}\n'
            f'  cleanup_success_destination:: '
            f'{self.cleanup_success_destination}\n'
            f'  collection:: {self.collection}\n'
            f'  data_sources:: {self.data_sources}\n'
            f'  data_source_extensions:: {self.data_source_extensions}\n'
            f'  failure_fqn:: {self.failure_fqn}\n'
            f'  failure_log_file_name:: {self.failure_log_file_name}\n'
            f'  features:: {self.features}\n'
            f'  interval:: {self.interval}\n'
            f'  log_file_directory:: {self.log_file_directory}\n'
            f'  log_to_file:: {self.log_to_file}\n'
            f'  logging_level:: {self.logging_level}\n'
            f'  netrc_file:: {self.netrc_file}\n'
            f'  observable_directory:: {self.observable_directory}\n'
            f'  observe_execution:: {self.observe_execution}\n'
            f'  progress_file_name:: {self.progress_file_name}\n'
            f'  progress_fqn:: {self.progress_fqn}\n'
            f'  proxy_file_name:: {self.proxy_file_name}\n'
            f'  proxy_fqn:: {self.proxy_fqn}\n'
            f'  recurse_data_sources:: {self.recurse_data_sources}\n'
            f'  rejected_directory:: {self.rejected_directory}\n'
            f'  rejected_file_name:: {self.rejected_file_name}\n'
            f'  rejected_fqn:: {self.rejected_fqn}\n'
            f'  report_fqn:: {self.report_fqn}\n'
            f'  resource_id:: {self.resource_id}\n'
            f'  retry_count:: {self.retry_count}\n'
            f'  retry_failures:: {self.retry_failures}\n'
            f'  retry_file_name:: {self.retry_file_name}\n'
            f'  retry_fqn:: {self.retry_fqn}\n'
            f'  slack_channel:: {self.slack_channel}\n'
            f'  slack_token:: secret\n'
            f'  source_host:: {self.source_host}\n'
            f'  state_fqn:: {self.state_fqn}\n'
            f'  storage_inventory_resource_id:: '
            f'{self.storage_inventory_resource_id}\n'
            f'  store_modified_files_only:: {self.store_modified_files_only}\n'
            f'  stream:: {self.stream}\n'
            f'  success_fqn:: {self.success_fqn}\n'
            f'  success_log_file_name:: {self.success_log_file_name}\n'
            f'  tap_id:: {self.tap_id}\n'
            f'  task_types:: {self.task_types}\n'
            f'  use_local_files:: {self.use_local_files}\n'
            f'  work_fqn:: {self.work_fqn}\n'
            f'  working_directory:: {self.working_directory}'
        )

    @staticmethod
    def _obtain_list(key, config, default=[]):
        """Make the configuration file entries into the Enum."""
        result = default
        if key in config:
            for ii in config[key]:
                result.append(ii)
        return list(set(result))

    @staticmethod
    def _obtain_task_types(config, default=None):
        """Make the configuration file entries into the Enum."""
        task_types = []
        if 'task_types' in config:
            for ii in config['task_types']:
                task_types.append(TaskType(ii))
            return task_types
        else:
            return default

    @staticmethod
    def _obtain_features(config):
        """Make the configuration file entries into the class members."""
        feature_flags = Features()
        if 'features' in config:
            for ii in config['features']:
                feature = f'_{ii}'
                if hasattr(feature_flags, feature):
                    setattr(feature_flags, feature, config['features'][ii])
                else:
                    logging.warning(f'Unexpected features item:{ii}.')
        return feature_flags

    def get(self):
        """Look up the configuration values in the data structure extracted
        from the configuration file."""
        return self.get_executors()

    def get_executors(self):
        """Look up the configuration values in the data structure extracted
        from the configuration file.

        Consider this deprecated - use get instead, because the name is
        non-representative of the work being done.
        """
        try:
            config = self.get_config()
            self.working_directory = config.get(
                'working_directory', os.getcwd()
            )
            self.work_file = config.get('todo_file_name', 'todo.txt')
            self.netrc_file = config.get('netrc_filename', None)
            self.data_sources = Config._obtain_list('data_sources', config, [])
            self.data_source_extensions = Config._obtain_list(
                'data_source_extensions', config, ['.fits']
            )
            self.resource_id = config.get(
                'resource_id', 'ivo://cadc.nrc.ca/sc2repo'
            )
            self.tap_id = config.get('tap_id', 'ivo://cadc.nrc.ca/sc2tap')
            self.use_local_files = bool(config.get('use_local_files', False))
            self.cleanup_failure_destination = config.get(
                'cleanup_failure_destination', None
            )
            self.cleanup_files_when_storing = bool(
                config.get('cleanup_files_when_storing', False)
            )
            self.cleanup_success_destination = config.get(
                'cleanup_success_destination', None
            )
            self.logging_level = config.get('logging_level', 'DEBUG')
            self.log_to_file = config.get('log_to_file', False)
            self.log_file_directory = config.get(
                'log_file_directory', self.working_directory
            )
            self.stream = config.get('stream', 'raw')
            self.task_types = Config._obtain_task_types(config, [])
            self.collection = config.get('collection', 'TEST')
            self.archive = config.get('archive', self.collection)
            self.success_log_file_name = config.get(
                'success_log_file_name', 'success_log.txt'
            )
            self.failure_log_file_name = config.get(
                'failure_log_file_name', 'failure_log.txt'
            )
            self.retry_file_name = config.get('retry_file_name', 'retries.txt')
            self.retry_failures = config.get('retry_failures', False)
            self.retry_count = config.get('retry_count', 1)
            self.rejected_file_name = config.get(
                'rejected_file_name', 'rejected.yml'
            )
            self.rejected_directory = config.get(
                'rejected_directory', os.getcwd()
            )
            self.progress_file_name = config.get(
                'progress_file_name', 'progress.txt'
            )
            self.interval = config.get('interval', 10)
            self.features = self._obtain_features(config)
            self.proxy_file_name = config.get('proxy_file_name', None)
            self.state_file_name = config.get('state_file_name', None)
            self.cache_file_name = config.get('cache_file_name', None)
            self.observe_execution = config.get('observe_execution', False)
            self.observable_directory = config.get(
                'observable_directory', None
            )
            self.recurse_data_sources = config.get(
                'recurse_data_sources', False
            )
            self.slack_channel = config.get('slack_channel', None)
            self.slack_token = config.get('slack_token', None)
            self.source_host = config.get('source_host', None)
            self.storage_inventory_resource_id = config.get(
                'storage_inventory_resource_id',
                'ivo://cadc.nrc.ca/global/raven',
            )
            self.store_modified_files_only = config.get(
                'store_modified_files_only', False
            )
            self._report_fqn = os.path.join(
                self.log_file_directory,
                f'{os.path.basename(self.working_directory)}_report.txt',
            )
        except KeyError as e:
            raise CadcException(f'Error in config file {e}')

        logger = logging.getLogger()
        logger.setLevel(self.logging_level)
        logging.debug(self)

    def get_config(self):
        """Return a configuration dictionary. Assumes a file named config.yml
        in the current working directory."""
        config_fqn = os.path.join(os.getcwd(), 'config.yml')
        config = self.load_config(config_fqn)
        if config is None:
            raise CadcException(f'Could not find the file {config_fqn}')
        return config

    def count_retries(self):
        result = []
        if os.path.exists(self.retry_fqn):
            with open(self.retry_fqn) as f:
                result = f.readlines()
        return len(result)

    def need_to_retry(self):
        """Evaluate the need to have the pipeline try to re-execute for any
         files/observations that have been logged as failures.

        If log_to_file is not set to True, there is no retry file content
        to retry on.

         :return True if the configuration and logging information indicate a
            need to attempt to retry the pipeline execution for any entries.
        """
        result = True
        if (
            self.features is not None
            and self.features.expects_retry
            and self.retry_failures
            and self.log_to_file
        ):
            meta = get_file_meta(self.retry_fqn)
            if meta['size'] == 0:
                logging.info(
                    f'Checked the retry file {self.retry_fqn}. There are no '
                    f'logged failures.'
                )
                result = False
        else:
            result = False
        return result

    def update_for_retry(self, count):
        """
        When retrying, the application will:

        - use the retries.txt file as the todo list
        - retry as many times as the 'retry count' in the config.yml file.
        - make a new log directory, in the working directory, with the name
            logs_{retry_count}. Any failures for the retry execution that
            need to be logged will be logged here.
        - in the new log directory, make a new .xml file for the
            output, with the name {obs_id}.xml

        :param count the current retry iteration
        """
        self.work_file = self.retry_file_name
        self.work_fqn = self.retry_fqn
        if '_' in os.path.basename(self.log_file_directory):
            temp = self.log_file_directory.rsplit('_', 1)[0]
            self.log_file_directory = f'{temp}_{count}'
        else:
            self.log_file_directory = f'{self.log_file_directory}_{count}'
        # reset the location of the log file names
        self.success_log_file_name = self.success_log_file_name
        self.failure_log_file_name = self.failure_log_file_name
        self.retry_file_name = self.retry_file_name

        logging.info(f'Retry work file is {self.work_fqn}')
        logging.debug(
            f'Retry logging files are:\n  '
            f'success: {self.success_fqn}\n  '
            f'failure: {self.failure_fqn}\n  '
            f'retry:   {self.retry_fqn}'
        )

    @staticmethod
    def load_config(config_fqn):
        """Read a configuration as a YAML file.
        :param config_fqn the fully qualified name for the configuration
            file.
        """
        try:
            logging.debug(f'Begin load_config from {config_fqn}.')
            with open(config_fqn) as f:
                data_map = yaml.safe_load(f)
                logging.debug('End load_config.')
                return data_map
        except (yaml.scanner.ScannerError, FileNotFoundError) as e:
            logging.error(e)
            return None

    @staticmethod
    def write_to_file(config):
        """Avoid specifying types when writing a config.yml file."""
        config_fqn = os.path.join(os.getcwd(), 'config.yml')
        with open(config_fqn, 'w') as f:
            for entry in dir(config):
                try:
                    attribute = getattr(config, entry)
                except TypeError:
                    pass
                if entry.startswith('_') or callable(attribute):
                    continue
                elif entry == 'features':
                    f.write('features:\n')
                    for feature in dir(attribute):
                        try:
                            feature_attribute = getattr(attribute, feature)
                        except TypeError:
                            pass
                        if feature.startswith('_') or callable(
                            feature_attribute
                        ):
                            continue
                        f.write(f'  {feature}: {feature_attribute}\n')
                elif entry == 'task_types':
                    if len(attribute) > 0:
                        f.write('task_types:\n')
                        for task in attribute:
                            f.write(f'  - {task.name.lower()}\n')
                elif entry == 'logging_level':
                    lookup = {
                        logging.DEBUG: 'DEBUG',
                        logging.INFO: 'INFO',
                        logging.WARNING: 'WARNING',
                        logging.ERROR: 'ERROR',
                    }
                    temp = lookup.get(attribute)
                    f.write(f'{entry}: {temp}\n')
                elif attribute is not None:
                    f.write(f'{entry}: {attribute}\n')


@dataclass
class PreviewMeta:
    f_name: str
    product_type: ProductType
    release_type: ReleaseType
    mime_type: str = 'image/jpeg'


class PreviewVisitor:
    """
    Common code for creating thumbnails and previews. Must be extended with
    the code that does the actual generation of thumbnail and preview
    images.

    This code takes care of adding artifacts to the Observation, placing the
    files in CADC storage, and cleaning up things left behind on disk.
    """

    def __init__(
        self, archive, release_type=None, mime_type='image/jpeg', **kwargs
    ):
        self._archive = archive
        self._release_type = release_type
        self._mime_type = mime_type
        self._logger = logging.getLogger(self.__class__.__name__)
        self._working_dir = kwargs.get('working_directory', './')
        self._cadc_client = kwargs.get('cadc_client')
        if self._cadc_client is None:
            self._logger.warning(
                'Visitor needs a cadc_client parameter to store previews.'
            )
        self._stream = kwargs.get('stream')
        if self._stream is None:
            self._logger.warning('No stream parameter.')
        self._observable = kwargs.get('observable')
        if self._observable is None:
            raise CadcException('Visitor needs a observable parameter.')
        self._storage_name = kwargs.get('storage_name')
        if self._storage_name is None:
            raise CadcException('Visitor needs a storage_name parameter.')
        self._science_file = self._storage_name.file_name
        self._science_fqn = self._storage_name.get_file_fqn(self._working_dir)
        self._delete_list = []
        # keys are uris, values are lists, where the 0th entry is a file name,
        # and the 1th entry is the artifact type
        self._previews = {}
        self._logger.debug(self)

    def __str__(self):
        return (
            f'working directory: {self._working_dir}\n'
            f'stream: {self._stream}\n'
            f'science file: {self._storage_name.file_name}\n'
        )

    def visit(self, observation):
        check_param(observation, Observation)
        count = 0
        if self._storage_name.product_id in observation.planes.keys():
            plane = observation.planes[self._storage_name.product_id]
            if self._storage_name.file_uri in plane.artifacts.keys():
                count += self._do_prev(plane, observation.observation_id)
            self._augment_artifacts(plane)
            self._delete_list_of_files()
        self._logger.info(
            f'Completed preview augmentation for {observation.observation_id}.'
        )
        return {'artifacts': count}

    def add_preview(
        self,
        uri,
        f_name,
        product_type,
        release_type=None,
        mime_type='image/jpeg',
    ):
        preview_meta = PreviewMeta(
            f_name, product_type, release_type, mime_type
        )
        self._previews[uri] = preview_meta

    def add_to_delete(self, fqn):
        self._delete_list.append(fqn)

    def _augment_artifacts(self, plane):
        """Add/update the artifact metadata in the plane."""
        for uri, entry in self._previews.items():
            temp = None
            if uri in plane.artifacts:
                temp = plane.artifacts[uri]
            f_name = entry.f_name
            product_type = entry.product_type
            release_type = self._release_type
            if self._release_type is None:
                release_type = entry.release_type
            fqn = os.path.join(self._working_dir, f_name)
            plane.artifacts[uri] = get_artifact_metadata(
                fqn, product_type, release_type, uri, temp
            )

    def _delete_list_of_files(self):
        """Clean up files on disk after."""
        # cadc_client will be None if executing a ScrapeModify task, so
        # leave the files behind so the user can see them on disk.
        if self._cadc_client is not None:
            for entry in self._delete_list:
                if os.path.exists(entry):
                    self._logger.warning(f'Deleting {entry}')
                    os.unlink(entry)

    def _do_prev(self, plane, obs_id):
        self.generate_plots(obs_id)
        self._store_smalls()
        return len(self._previews)

    def generate_plots(self, obs_id):
        raise NotImplementedError

    @property
    def storage_name(self):
        return self._storage_name

    def _store_smalls(self):
        if self._cadc_client is not None:
            for uri, entry in self._previews.items():
                self._cadc_client.put(
                    self._working_dir, uri, self._stream
                )


class StorageName:
    """
    This class encapsulates:
    - naming rules for a collection, for example:
        - file name case in storage,
        - compression extensions
        - naming pattern enforcement
    - cardinality rules for a collection. Specialize for creation support of:
        - observation_id
        - product_id
        - artifact URI
        - preview URI
        - thumbnail URI
    - fully-qualified name of a file at it's source, if required. This
      may be a Linux directory+file name, and HTTP URL, or an IVOA Virtual
      Storage URI.
    """

    def __init__(
        self,
        obs_id=None,
        collection=None,
        collection_pattern='.*',
        fname_on_disk=None,
        scheme='ad',
        url=None,
        mime_encoding=None,
        mime_type='application/fits',
        compression='.gz',
        entry=None,
        source_names=[],
        destination_uris=[],
    ):
        """

        :param obs_id: string value for Observation.observationID
        :param collection: string value for Observation.collection
        :param collection_pattern: regular expression that can be used to
            determine if a file name or observation id meets particular
            patterns.
        :param fname_on_disk: string value for the name of a file on disk,
            which is not necessarily the same thing as the name of the file
            in storage (i.e. extensions may exist in one location that do
            not exist in another).
        :param scheme: string value for the scheme of the file URI.
        :param url: if the metadata/data is externally available via http,
            the url for retrieval
        :param entry: string - the value as obtained from the DataSource,
            unchanged for use in the retries.txt file.
        :param source_names: list of str - the fully-qualified representation
            of files, as represented at the source. Sufficient for retrieval,
            probably includes a scheme.
        :param destination_uris: list of str - the Artifact URIs as
            represented at CADC. Sufficient for storing/retrieving to/from
            CADC.
        :param scheme: str, should eventually default to 'cadc'
        :param mime_encoding: str, used for CADC /data storage
        :param mime_type: str, used for CADC /data storage
        """
        self.obs_id = obs_id
        self.collection = collection
        self.collection_pattern = collection_pattern
        self.scheme = scheme
        self.fname_on_disk = fname_on_disk
        self._url = url
        self._mime_encoding = mime_encoding
        self._mime_type = mime_type
        self._compression = compression
        self._source_names = source_names
        self._destination_uris = destination_uris
        self._entry = entry
        self._logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        return (
            f'\n'
            f'          obs_id: {self.obs_id}\n'
            f'       file_name: {self.file_name}\n'
            f'        file_uri: {self.file_uri}\n'
            f'         lineage: {self.lineage}\n'
            f'      product_id: {self.product_id}\n'
            f'    source_names: {self.source_names}\n'
            f'destination_uris: {self.destination_uris}'
        )

    @property
    def entry(self):
        return self._entry

    @property
    def file_uri(self):
        """The ad URI for the file. Assumes compression."""
        return (
            f'{self.scheme}:{self.collection}/{self.file_name}'
            f'{self._compression}'
        )

    @property
    def file_name(self):
        """The file name."""
        return f'{self.obs_id}.fits'

    @property
    def compressed_file_name(self):
        """The compressed file name - adds the .gz extension."""
        return f'{self.obs_id}.fits{self._compression}'

    @property
    def destination_uris(self):
        return self._destination_uris

    @destination_uris.setter
    def destination_uris(self, value):
        self._destination_uris = value

    @property
    def model_file_name(self):
        """The file name used on local disk that holds the CAOM2 Observation
        XML."""
        return f'{self.obs_id}.xml'

    @property
    def prev(self):
        """The preview file name for the file."""
        return f'{self.obs_id}_prev.jpg'

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return f'{self.obs_id}_prev_256.jpg'

    @property
    def prev_uri(self):
        """The preview URI."""
        return self._get_uri(self.prev)

    @property
    def thumb_uri(self):
        """The thumbnail URI."""
        return self._get_uri(self.thumb)

    @property
    def obs_id(self):
        """The observation ID associated with the file name."""
        return self._obs_id

    @obs_id.setter
    def obs_id(self, value):
        self._obs_id = value

    @property
    def log_file(self):
        """The log file name used when running any of the 'execute' steps."""
        return f'{self.obs_id}.log'

    @property
    def product_id(self):
        """The relationship between the observation ID of an observation, and
        the product ID of a plane."""
        return self.obs_id

    @property
    def fname_on_disk(self):
        """The file name on disk, which is not necessarily the same as the
        file name in ad."""
        return self._fname_on_disk

    @property
    def url(self):
        """A URL from where a file can be retrieved."""
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    @property
    def mime_encoding(self):
        """The mime encoding for the file, defaults to None."""
        return self._mime_encoding

    @mime_encoding.setter
    def mime_encoding(self, value):
        self._mime_encoding = value

    @property
    def mime_type(self):
        """The mime type for the file, defaults to application/fits."""
        return self._mime_type

    @mime_type.setter
    def mime_type(self, value):
        self._mime_type = value

    @property
    def lineage(self):
        """The value provided to the --lineage parameter for
        fits2caom2 extensions."""
        return f'{self.product_id}/{self.file_uri}'

    @property
    def source_names(self):
        return self._source_names

    @source_names.setter
    def source_names(self, value):
        self._source_names = value

    @property
    def external_urls(self):
        return None

    @property
    def is_feasible(self):
        """
        To support the exclusion of CFHT HDF5 files in the pipeline.
        :return:
        """
        return True

    def multiple_files(self, config=None):
        return []

    @fname_on_disk.setter
    def fname_on_disk(self, value):
        self._fname_on_disk = value

    def is_valid(self):
        """:return True if the observation ID conforms to naming rules."""
        pattern = re.compile(self.collection_pattern)
        return pattern.match(self.obs_id)

    def _get_uri(self, fname):
        """
        The Artifact URI for a file, without consideration for compression.
        """
        return f'{self.scheme}:{self.collection}/{fname}'

    def get_file_fqn(self, working_directory):
        if (
            self._source_names is not None and
            len(self._source_names) > 0 and
            os.path.exists(self._source_names[0])
        ):
            fqn = self._source_names[0]
        else:
            fqn = os.path.join(working_directory, self.file_name)
        return fqn

    @staticmethod
    def remove_extensions(name):
        """How to get the file_id from a file_name."""
        return (
            name.replace('.fits', '').replace('.gz', '').replace('.header', '')
        )

    @staticmethod
    def is_hdf5(entry):
        return '.hdf5' in entry or '.h5' in entry

    @staticmethod
    def is_preview(entry):
        return '.jpg' in entry


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
    """

    def __init__(
        self,
        source_name,
        scheme='ad',
        preview_suffix='jpg',
        source_tz=timezone.utc,
    ):
        """

        :param source_name: String value used for logging
        :param scheme: string which encapsulates scheme as used in CAOM
            Artifact URIs. The default of 'ad' means the canonical version
            of the file is stored at CADC.
        :param preview_suffix String value that is excluded from queries,
            because it's produced at CADC, so something like '256.jpg' should
            work for Gemini.
        :param source_tz String representation of timezone name, as understood
            by pytz.
        """
        self._config = Config()
        self._config.get_executors()
        self._source = []
        self._destination_data = []
        self._destination_meta = []
        self._source_name = source_name
        self._scheme = scheme
        self._preview_suffix = preview_suffix
        self._source_tz = source_tz
        self._logger = logging.getLogger(self.__class__.__name__)

    def _filter_result(self):
        """The default implementation does nothing, but this allows
        implementations that extend this class to remove entries from
        the comparison results for whatever reason might come up."""
        pass

    def _find_unaligned_dates(self, source, meta, data):
        result = set()
        if len(data) > 0:
            # AD - 2019-11-18 - 'ad' timezone is US/Pacific
            dest_tz = tz.gettz('US/Pacific')
            for f_name in meta:
                if f_name in source and f_name in data['fileName']:
                    source_dt = datetime.fromtimestamp(source[f_name])
                    source_in_tz = source_dt.replace(tzinfo=self._source_tz)
                    source_utc = source_in_tz.astimezone(timezone.utc)
                    mask = data['fileName'] == f_name
                    # 0 - only one row in the mask
                    # 1 - timestamps are the second column
                    dest_dt_orig = data[mask][0][1]
                    dest_dt = datetime.strptime(dest_dt_orig, ISO_8601_FORMAT)
                    dest_pac = dest_dt.replace(tzinfo=dest_tz)
                    dest_utc = dest_pac.astimezone(timezone.utc)
                    if dest_utc < source_utc:
                        result.add(f_name)
        return result

    def _read_list_from_destination_data(self):
        """Code to execute a query for files and the arrival time, that are in
        CADC storage.
        """
        ad_resource_id = 'ivo://cadc.nrc.ca/ad'
        query = (
            f"SELECT fileName, ingestDate FROM archive_files WHERE "
            f"archiveName = '{self._config.archive}' "
            f"AND fileName not like '%{self._preview_suffix}'"
        )
        self._logger.debug(f'Query is {query}')
        return query_tap(query, self._config.proxy_fqn, ad_resource_id)

    def _read_list_from_destination_meta(self):
        query = (
            f"SELECT A.uri FROM caom2.Observation AS O "
            f"JOIN caom2.Plane AS P ON O.obsID = P.obsID "
            f"JOIN caom2.Artifact AS A ON P.planeID = A.planeID "
            f"WHERE O.collection='{self._config.collection}' "
            f"AND A.uri not like '%{self._preview_suffix}'"
        )
        self._logger.debug(f'Query is {query}')
        temp = query_tap(query, self._config.proxy_fqn, self._config.tap_id)
        return Validator.filter_meta(temp)

    def read_from_source(self):
        """Read the entire source site listing. This function is expected to
        return a dict of all the file names available from the source, where
        the keys are the file names, and the values are the timestamps at the
        source."""
        raise NotImplementedError()

    def validate(self):
        self._logger.info('Query destination metadata.')
        dest_meta_temp = self._read_list_from_destination_meta()

        self._logger.info('Query source metadata.')
        source_temp = self.read_from_source()

        self._logger.info('Find files that do not appear at CADC.')
        self._destination_meta = find_missing(
            dest_meta_temp, source_temp.keys()
        )

        self._logger.info(
            f'Find files that do not appear at {self._source_name}.'
        )
        self._source = find_missing(source_temp.keys(), dest_meta_temp)

        self._logger.info('Query destination data.')
        dest_data_temp = self._read_list_from_destination_data()

        self._logger.info(
            f'Find files that are newer at {self._source_name} '
            f'than at CADC.'
        )
        self._destination_data = self._find_unaligned_dates(
            source_temp, dest_meta_temp, dest_data_temp
        )

        self._logger.info(f'Filter the results.')
        self._filter_result()

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
        return [CaomName(ii.strip()).file_name for ii in meta['uri']]


def compare_observations(actual_fqn, expected_fqn):
    """Compare the observation captured in actual_fqn with the observation
    captured in the expected_fqn. Returns the differences as a pretty
    string for logging.
    """
    actual = read_obs_from_file(actual_fqn)
    expected = read_obs_from_file(expected_fqn)
    result = get_differences(expected, actual, 'Observation')
    msg = None
    if result:
        compare_text = '\n'.join([r for r in result])
        msg = (
            f'Differences found in observation {expected.observation_id}\n'
            f'{compare_text}'
        )
    return msg


def to_float(value):
    """Cast to float, without throwing an exception."""
    result = None
    if value is not None:
        if isinstance(value, float):  #  or isinstance(value, int):
            result = value
        elif (isinstance(value, str) and len(value.strip()) > 0) or (
            isinstance(value, int)
        ):
            result = float(value)
    return result


def to_int(value):
    """Cast to int, without throwing an exception."""
    return int(value) if value is not None else None


def to_str(value):
    """Cast to str, without throwing an exception."""
    return str(value) if value is not None else None


def extract_file_name_from_uri(uri_str):
    return parse.urlparse(uri_str).path.split('/')[-1]


def exec_cmd(cmd, log_level_as=logging.debug, timeout=None):
    """
    This does command execution as a subprocess call.

    :param cmd the text version of the command being executed
    :param log_level_as control the logging level from the exec call
    :param timeout value in seconds, after which the process is terminated
        raising the TimeoutExpired exception.
    :return None
    """
    logging.debug(cmd)
    cmd_array = cmd.split()
    try:
        child = subprocess.Popen(
            cmd_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            output, outerr = child.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logging.warning(f'Command {cmd_array} timed out.')
            # child process is not killed if the timeout expires, so kill the
            # process and finish communication
            child.kill()
            child.stdout.close()
            child.stderr.close()
            output, outerr = child.communicate()
        if len(output) > 0:
            log_level_as(f'stdout {output.decode("utf-8")}')
        if len(outerr) > 0:
            logging.error(f'stderr {outerr.decode("utf-8")}')
        if child.returncode != 0 and child.returncode != -9:
            # not killed due to a timeout
            logging.warning(f'Command {cmd} failed with {child.returncode}.')
            raise CadcException(
                f'Command {cmd} ::\nreturncode {child.returncode}, \nstdout '
                f'{output.decode("utf-8")}\' \nstderr '
                f'{outerr.decode("utf-8")}\''
            )
    except Exception as e:
        if isinstance(e, CadcException):
            raise e
        logging.warning(f'Error with command {cmd}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(f'Could not execute cmd {cmd}. Exception {e}')


def exec_cmd_info(cmd):
    """
    This does command execution as a subprocess call.

    :param cmd the text version of the command being executed
    :return The text from stdout.
    """
    logging.debug(cmd)
    cmd_array = cmd.split()
    try:
        output, outerr = subprocess.Popen(
            cmd_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()
        if outerr is not None and len(outerr) > 0 and outerr[0] is not None:
            raise CadcException(
                f'Command {cmd} had stderr {outerr.decode("utf-8")}'
            )
        if output is not None and len(output) > 0:
            return output.decode('utf-8')
    except Exception as e:
        logging.debug(f'Error with command {cmd}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(f'Could not execute cmd {cmd}. Exception {e}')


def exec_cmd_redirect(cmd, fqn):
    """
    This does command execution as a subprocess call. It redirects stdout
    to fqn, and assumes binary output for the re-direct.

    :param cmd the text version of the command being executed
    :param fqn the fully-qualified name of the file to which stdout is
        re-directed
    :return None
    """
    logging.debug(cmd)
    cmd_array = cmd.split()
    try:
        with open(fqn, 'wb') as outfile:
            outerr = subprocess.Popen(
                cmd_array, stdout=outfile, stderr=subprocess.PIPE
            ).communicate()
            if (
                outerr is not None
                and len(outerr) > 0
                and outerr[0] is not None
            ):
                logging.debug(
                    f'Command {cmd} had stderr {outerr.decode("utf-8")}'
                )
                raise CadcException(
                    f'Command {cmd} had outerr {outerr.decode("utf-8")}'
                )
    except Exception as e:
        logging.debug(f'Error with command {cmd}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(f'Could not execute cmd {cmd}.Exception {e}')


def ftp_get(ftp_host_name, source_fqn, dest_fqn):
    """
    :param ftp_host_name name from which to originate the FTP transfer. Assume
        anonymous login.
    :param source_fqn fully-qualified name on the FTP host, of the file to be
        transferred.
    :param dest_fqn fully-qualified name, locally valid, for where to transfer
        the file to.

    Uses ftputil, which always transfers files in binary mode.
    """
    # remove the need to have ftputil libraries on EVERY *2caom2 pipeline
    from ftputil import FTPHost

    try:
        with FTPHost(ftp_host_name, 'anonymous', '@anonymous') as ftp_host:
            ftp_host.download(source_fqn, dest_fqn)
            source_stats = ftp_host.stat(source_fqn)
            ftp_host.close()
            dest_meta = get_file_meta(dest_fqn)
            if source_stats.st_size == dest_meta.get('size'):
                logging.info(f'Downloaded {source_fqn} from {ftp_host_name}')
            else:
                os.unlink(dest_fqn)
                raise CadcException(
                    f'File size error when transferring {source_fqn} from '
                    f'{ftp_host_name}'
                )
    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not transfer {source_fqn} from {ftp_host_name}'
        )


def ftp_get_timeout(ftp_host_name, source_fqn, dest_fqn, timeout=20):
    """
    :param ftp_host_name name from which to originate the FTP transfer. Assume
        anonymous login.
    :param source_fqn fully-qualified name on the FTP host, of the file to be
        transferred.
    :param dest_fqn fully-qualified name, locally valid, for where to transfer
        the file to.
    :param timeout in seconds for blocking operations

    Uses ftplib, which supports specifying timeouts in the connection.
    """
    # remove the need to have ftputil libraries on EVERY *2caom2 pipeline
    from ftplib import FTP

    try:
        with FTP(ftp_host_name, timeout=timeout) as ftp_host:
            ftp_host.login()
            with open(dest_fqn, 'wb') as fp:
                ftp_host.retrbinary(f'RETR {source_fqn}', fp.write)
            ftp_host.voidcmd('TYPE I')
            source_size = ftp_host.size(source_fqn)
            ftp_host.quit()
            dest_meta = get_file_meta(dest_fqn)
            if source_size == dest_meta.get('size'):
                logging.info(f'Downloaded {source_fqn} from {ftp_host_name}')
            else:
                os.unlink(dest_fqn)
                raise CadcException(
                    f'File size error when transferring {source_fqn} from '
                    f'{ftp_host_name}'
                )
    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not transfer {source_fqn} from {ftp_host_name}'
        )


def get_cadc_headers(uri):
    """
    Creates the FITS headers object by fetching the FITS headers of a CADC
    file. The function takes advantage of the fhead feature of the CADC
    storage service and retrieves just the headers and no data, minimizing
    the transfer time.

    The file must be public, because the header retrieval is done as an
    anonymous user.

    :param uri: CADC URI
    :return: a string of keyword/value pairs.
    """
    file_url = parse.urlparse(uri)
    # create possible types of subjects
    subject = net.Subject()
    client = CadcDataClient(subject)
    # do a fhead on the file
    archive, file_id = file_url.path.split('/')
    return get_cadc_headers_client(archive, file_id, client)


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


def get_cadc_meta(netrc_fqn, archive, fname):
    """
    Gets contentType, contentLength and contentChecksum of a CADC artifact
    :param netrc_fqn: user credentials
    :param archive: archive file has been stored to
    :param fname: name of file in the archive
    :return:
    """
    subject = net.Subject(username=None, certificate=None, netrc=netrc_fqn)
    client = CadcDataClient(subject)
    return client.get_file_info(archive, fname)


def get_cadc_meta_client(client, archive, fname):
    """
    Gets contentType, contentLength and contentChecksum of a CADC artifact
    :param client: CadcDataClient instance
    :param archive: archive file has been stored to
    :param fname: name of file in the archive
    :return:
    """
    return client.get_file_info(archive, fname)


def get_file_meta(fqn):
    """
    Gets contentType, contentLength and contentChecksum of an artifact on disk.

    At the time of writing, libmagic gets confused with gzip'd FITS files,
    and identifies them as application/octet-stream, so, just have a bunch
    of hard-coding instead.

    :param fqn: Fully-qualified name of the file for which to get the metadata.
    :return:
    """
    if fqn is None or not os.path.exists(fqn):
        raise CadcException(f'Could not find {fqn} in get_file_meta')
    meta = {
        'size': get_file_size(fqn),
        'md5sum': md5(open(fqn, 'rb').read()).hexdigest(),
    }
    if fqn.endswith('.header') or fqn.endswith('.txt') or fqn.endswith('.cat'):
        meta['type'] = 'text/plain'
    elif fqn.endswith('.csv'):
        meta['type'] = 'text/csv'
    elif fqn.endswith('.gif'):
        meta['type'] = 'image/gif'
    elif fqn.endswith('.png'):
        meta['type'] = 'image/png'
    elif fqn.endswith('.jpg'):
        meta['type'] = 'image/jpeg'
    elif fqn.endswith('tar.gz'):
        meta['type'] = 'application/x-tar'
    elif fqn.endswith('h5') or fqn.endswith('hdf5'):
        meta['type'] = 'application/x-hdf5'
    else:
        meta['type'] = 'application/fits'
    logging.debug(meta)
    return meta


def get_file_size(fqn):
    """Get a file size on disk.

    :param fqn: Fully-qualified name of the file for which to get the size
    """
    s = os.stat(fqn)
    return s.st_size


def get_version(entry):
    """A common implementation to retrieve a pipeline version."""
    return f'{entry}/{version(entry)}'


def create_dir(dir_name):
    """Create the working area if it does not already exist."""
    if os.path.exists(dir_name):
        if not os.path.isdir(dir_name):
            raise CadcException(f'{dir_name} is not a directory.')
        if not os.access(dir_name, os.W_OK | os.X_OK):
            os.chmod(dir_name, stat.S_IRWXU)
            if not os.access(dir_name, os.W_OK | os.X_OK):
                raise CadcException(f'{dir_name} is not writeable.')
    else:
        os.makedirs(dir_name, mode=0o775)


def decompose_lineage(lineage):
    """Returns a product id and an artifact uri from the command line."""
    try:
        result = lineage.split('/', 1)
        return result[0], result[1]
    except Exception as e:
        logging.debug(
            f'Lineage {lineage} caused error {e}. Expected '
            f'product_id/ad:ARCHIVE/FILE_NAME'
        )
        logging.debug(traceback.format_exc())
        raise CadcException('Expected product_id/ad:ARCHIVE/FILE_NAME')


def decompose_uri(uri):
    """
    Returns a scheme, path (maybe an archive), and a file name from the
    command line.
    """
    try:
        temp = uri.split(':', 1)
        scheme = temp[0]
        temp1 = temp[1].rsplit('/', 1)
        path = temp1[0]
        file_name = temp1[1]
        return scheme, path, file_name
    except Exception as e:
        logging.debug(
            f'URI {uri} caused error {e}. Expected scheme:path/FILE_NAME'
        )
        logging.debug(traceback.format_exc())
        raise CadcException(f'Expected scheme:path/FILE_NAME. Got {uri}.')


def check_param(param, param_type):
    """Generic code to check if a parameter is not None, and is of the
    expected type.
    """
    if param is None or not isinstance(param, param_type):
        raise CadcException(f'Parameter {param} failed check for {param_type}')


def read_csv_file(fqn):
    """Read a csv file.

    :returns a list of lists.
    """
    results = []
    try:
        with open(fqn) as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                results.append(row)
    except Exception as e:
        logging.error(f'Could not read from csv file {fqn}')
        logging.debug(traceback.format_exc())
        raise CadcException(e)
    return results


def write_obs_to_file(obs, fqn):
    """Common code to write a CAOM Observation to a file."""
    ow = ObservationWriter()
    ow.write(obs, fqn)


def read_obs_from_file(fqn):
    """Common code to read a CAOM Observation from a file."""
    if not os.path.exists(fqn):
        raise CadcException(f'Could not find {fqn}')
    reader = ObservationReader(False)
    return reader.read(fqn)


def read_from_file(fqn):
    """Common code to read from a text file. Mostly to make it easy to
    mock.
    """
    if not os.path.exists(fqn):
        raise CadcException(f'Could not find {fqn}')
    with open(fqn) as f:
        return f.readlines()


def write_to_file(fqn, content):
    """Common code to write to a fully-qualified file name. Mostly to make
    it easy to mock.
    """
    try:
        with open(fqn, 'w') as f:
            f.write(content)
    except Exception:
        logging.error(f'Could not write file {fqn}')
        raise CadcException(f'Could not write file {fqn}')


def update_typed_set(typed_set, new_set):
    """Common code to remove all the entries from an existing set, and
    then replace those entries with a new set.
    """
    # remove the previous values
    while len(typed_set) > 0:
        typed_set.pop()
    typed_set.update(new_set)


def format_time_for_query(from_time):
    length = len(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    return datetime.strptime(from_time[:length], '%Y-%m-%dT%H:%M:%S')


def read_file_list_from_archive(config, app_name, prev_exec_date, exec_date):
    """Code to execute a time-boxed query for files that have arrived in ad.

    :param config Config instance
    :param app_name Information used in the http connection for tracing
        queries.
    :param prev_exec_date Timestamp that indicates the beginning of the
        chunk of time. Results will be > than this time.
    :param exec_date Timestamp. that indicates the end of the chunk of time.
        Results will be <= this time.
    """
    start_time = format_time_for_query(prev_exec_date)
    end_time = format_time_for_query(exec_date)
    ad_resource_id = 'ivo://cadc.nrc.ca/ad'
    query = (
        f"SELECT fileName, min(ingestDate) FROM archive_files WHERE "
        f"archiveName = '{config.archive}' AND ingestDate > "
        f"'{start_time}' and "
        f"ingestDate <= '{end_time}' ORDER BY ingestDate "
        f"GROUP BY ingestDate"
    )
    logging.debug(f'Query is {query}')
    temp = query_tap(query, config.proxy_fqn, config.resource_id)
    return [ii for ii in temp['fileName']]


def get_keyword(headers, keyword):
    result = headers[0].get(keyword)
    if result is None and len(headers) > 1:
        result = headers[1].get(keyword)
    return result


def get_lineage(archive, product_id, file_name, scheme='ad'):
    """Construct an instance of the caom2gen lineage parameter.
    :param archive archive name at CADC.
    :param product_id CAOM2 Plane unique identifier.
    :param file_name String representation of the file name.
    :param scheme Usually 'ad', otherwise an indication of external storage.
    :return str understood by the caom2gen application, lineage parameter
        value
    """
    return f'{product_id}/{scheme}:{archive}/{file_name}'


def get_artifact_metadata(
    fqn, product_type, release_type, uri=None, artifact=None
):
    """
    Build or update artifact content metadata using the CAOM2 objects, and
    with access to a file on disk.

    :param fqn: The fully-qualified name of the file on disk, for which an
        Artifact is being created or updated.
    :param product_type: which ProductType enumeration value
    :param release_type: which ReleaseType enumeration value
    :param uri: mandatory if creating an Artifact, a URI of the form
        scheme:ARCHIVE/file_name
    :param artifact: use when updating an existing Artifact instance

    :return: the created or updated Artifact instance, with the
        content_* elements filled in.
    """
    local_meta = get_file_meta(fqn)
    md5uri = ChecksumURI(f'md5:{local_meta["md5sum"]}')
    if artifact is None:
        if uri is None:
            raise CadcException('Cannot build an Artifact without a URI.')
        return Artifact(
            uri,
            product_type,
            release_type,
            local_meta['type'],
            local_meta['size'],
            md5uri,
        )
    else:
        artifact.product_type = product_type
        artifact.content_type = local_meta['type']
        artifact.content_length = local_meta['size']
        artifact.content_checksum = md5uri
        artifact.release_type = release_type
        return artifact


def get_artifact_metadata_client(
    client,
    file_name,
    product_type,
    release_type,
    archive,
    uri=None,
    artifact=None,
):
    """
    Build or update artifact content metadata using the CAOM2 objects, and
    with access to a file at CADC.

    :param client: CadcDataClient instance
    :param file_name: The name of the file in CADC storage, for which an
        Artifact is being created or updated.
    :param product_type: which ProductType enumeration value
    :param release_type: which ReleaseType enumeration value
    :param archive: str narrows down CADC storage location
    :param uri: mandatory if creating an Artifact, a URI of the form
        scheme:ARCHIVE/file_name
    :param artifact: use when updating an existing Artifact instance

    :return: the created or updated Artifact instance, with the
        content_* elements filled in.
    """
    meta = get_cadc_meta_client(client, archive, file_name)
    md5uri = ChecksumURI(f'md5:{meta.get("md5sum")}')
    if artifact is None:
        if uri is None:
            raise CadcException('Cannot build an Artifact without a URI.')
        return Artifact(
            uri,
            product_type,
            release_type,
            meta.get('type'),
            to_int(meta.get('size')),
            md5uri,
        )
    else:
        artifact.product_type = product_type
        artifact.content_type = meta.get('type')
        artifact.content_length = to_int(meta.get('size'))
        artifact.content_checksum = md5uri
        return artifact


def convert_to_days(exposure_time):
    """
    Converts from seconds to days.

    :param exposure_time:
    :return:
    """
    return exposure_time / (24.0 * 3600.0)


def convert_to_ts(value):
    """
    Converts to seconds since the epoch. Tries to be lenient about the
    type of the incoming value.
    :param value:
    :return: float that represents seconds since the epoch.
    """
    if isinstance(value, datetime):
        result = value.timestamp()
    elif isinstance(value, float):
        result = value
    else:
        result = make_seconds(value)
    return result


def sizeof(x):
    """Encapsulate returning the memory size in bytes."""
    return sys.getsizeof(x)


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
    start = datetime.utcnow().timestamp()
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
        raise CadcException(f'Failed to store data with {e}')
    finally:
        os.chdir(cwd)
    end = datetime.utcnow().timestamp()
    metrics.observe(start, end, file_size, 'put', 'data', file_name)


def build_uri(archive, file_name, scheme='ad'):
    """One location to keep the syntax for an Artifact URI."""
    return f'{scheme}:{archive}/{file_name}'


def query_endpoint(url, timeout=20):
    """Return a response for an endpoint. Caller needs to call 'close'
    on the response.
    """

    # Open the URL and fetch the JSON document for the observation
    session = requests.Session()
    retries = 10
    retry = Retry(
        total=retries, read=retries, connect=retries, backoff_factor=0.5
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.debug(traceback.format_exc())
        raise CadcException(f'Endpoint {url} failure {str(e)}')


def get_endpoint_session(retries=10, backoff_factor=0.5):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def query_endpoint_session(url, session, timeout=20):
    """Return a response for an endpoint. Caller needs to call 'close'
    on the response.
    """

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.debug(traceback.format_exc())
        raise CadcException(f'Endpoint {url} failure {str(e)}')


def read_as_yaml(fqn):
    """Read and return YAML content of 'fqn'."""
    try:
        logging.debug(f'Begin read_as_yaml for {fqn}.')
        with open(fqn) as f:
            data_map = yaml.safe_load(f)
            logging.debug('End read_as_yaml.')
            return data_map
    except (yaml.scanner.ScannerError, FileNotFoundError) as e:
        logging.error(e)
        return None


def write_as_yaml(content, fqn):
    """Write 'content' to 'fqn' as YAML."""
    try:
        logging.debug(f'Begin write_as_yaml for {fqn}.')
        with open(fqn, 'w') as f:
            yaml.dump(content, f, default_flow_style=False)
            logging.debug('End write_as_yaml.')
    except Exception as e:
        logging.debug(traceback.format_exc())
        logging.error(e)


def load_module(module, command_name):
    """
    Add code to the execution environment of the interpreter.

    :param module the fully-qualified path name to the source code.
    :param command_name what will be executed within the module.
    """
    mname = os.path.basename(module)
    if '.' in mname:
        # remove extension from the provided name
        mname = mname.split('.')[0]
    pname = os.path.dirname(module)
    sys.path.append(pname)
    try:
        logging.debug(f'Looking for {command_name} in {module}.')
        result = importlib.import_module(mname)
        if not hasattr(result, command_name):
            raise CadcException(
                f'Could not find command {command_name} in module {module}.'
            )
    except ImportError as e:
        logging.debug(f'Looking for {mname} in {pname}')
        raise e
    return result


def make_seconds(from_time):
    """Deal with different time formats to get the number of
    seconds since the epoch.

    Timezone information may be present as +00, strip that for returned
    results.
    :param from_time a string representing some time
    :return the time as a timestamp, so seconds.microseconds
    """
    try:
        index = from_time.index('+00')
    except ValueError:
        index = len(from_time)

    # OMM 2019/07/16 03:15:46
    # CADC Data Client Thu, 14 May 2020 20:29:02 GMT
    # NGVS Wed Mar 24 2010 16:10:36
    for fmt in [
        ISO_8601_FORMAT,
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%d-%b-%Y %H:%M',
        '%b %d %Y',
        '%b %d %H:%M',
        '%Y%m%d-%H%M%S',
        '%Y-%m-%d',
        '%Y-%m-%dHST%H:%M:%S',
        '%a %b %d %H:%M:%S HST %Y',
        '%Y/%m/%d %H:%M:%S',
        '%a, %d %b %Y %H:%M:%S GMT',
        '%Y-%m-%dT%H:%M',
        '%a %b %d %Y %H:%M:%S',
    ]:
        try:
            seconds_since_epoch = datetime.strptime(
                from_time[:index], fmt
            ).timestamp()
            if fmt == '%b %d %H:%M':
                # the format '%b %d %H:%M' results in a timestamp based on
                # 1900, so need to set it to 'this' year
                year = datetime.utcnow().year
                dt = f'{from_time[:index]} {year}'
                dt_format = f'{fmt} %Y'
                seconds_since_epoch = datetime.strptime(
                    dt, dt_format
                ).timestamp()
            if (
                fmt == '%Y-%m-%dHST%H:%M:%S'
                or fmt == '%a %b %d %H:%M:%S HST %Y'
            ):
                # change timezone from HST to UTC - add 10 hours -
                # accurate enough implementation for the CFHT
                # provenance.lastExecuted value
                seconds_since_epoch += 10 * 3600
            break
        except ValueError:
            seconds_since_epoch = None
    if seconds_since_epoch is None:
        raise CadcException(f'Could not make seconds from {from_time}')
    return seconds_since_epoch


def make_time(from_str):
    """Make a string into a datetime value.

    :param from_str a string representing some time.
    :return the time as a datetime
    """
    temp = make_seconds(from_str)
    result = None
    if temp is not None:
        result = datetime.utcfromtimestamp(temp)
    return result


def make_time_tz(from_value):
    """
    Make an offset-aware datetime value. Input parameters should be in
    datetime format, but a modest attempt is made to check for otherwise.

    Why is UTC ok?
    - OS times are all UTC, because they're all running in a Docker container
      with no timezone configured
    - make_seconds checks for time zones and adjusts there

    :param from_value a representation of time.
    :return the time as an offset-aware datetime

    from_value accepted types:
    - datetime, ensures timezone.utc is set
    - str, attempts to use datetime.strptime with all the formats so far
      encountered to convert it to a datetime, and then sets timezone.utc
    - float, uses as if the parameter were the result of a datetime.timestamp()
      operation, as well as setting timezone.utc
    - datetime.date or datetime.time, sets timezone.utc
    """
    if isinstance(from_value, datetime):
        result = from_value
        if from_value.tzinfo is None:
            result = from_value.replace(tzinfo=timezone.utc)
    elif isinstance(from_value, str):
        temp = make_seconds(from_value)
        result = None
        if temp is not None:
            result = datetime.fromtimestamp(temp, tz=timezone.utc)
    elif isinstance(from_value, float):
        result = datetime.fromtimestamp(from_value, tz=timezone.utc)
    else:
        result = from_value.fromtimestamp(from_value, tz=timezone.utc)
    return result


def increment_time(this_ts, by_interval, unit='%M'):
    """
    Increment time by an interval. Times should be in datetime format, but
    a modest attempt is made to check for otherwise.

    :param this_ts: datetime
    :param by_interval: integer - e.g. 10, for a 10 minute increment
    :param unit: the formatting string, default is minutes
    :return: this_ts incremented by interval amount
    """
    if isinstance(this_ts, datetime):
        time_s = this_ts.timestamp()
    elif isinstance(this_ts, str):
        time_s = make_seconds(this_ts)
    else:
        time_s = this_ts
    if unit == '%M':
        factor = 60
    else:
        raise NotImplementedError(f'Unexpected unit {unit}')
    interval = by_interval * factor
    temp = time_s + interval
    return datetime.fromtimestamp(temp)


def increment_time_tz(this_ts, by_interval, unit='%M'):
    """
    Increment time by an interval. Times should be in datetime format, but
    a modest attempt is made to check for otherwise.

    :param this_ts: datetime
    :param by_interval: integer - e.g. 10, for a 10 minute increment
    :param unit: the formatting string, default is minutes
    :return: this_ts incremented by interval amount. Offset-aware.
    """
    if isinstance(this_ts, datetime):
        time_dt = this_ts
    elif isinstance(this_ts, str):
        time_dt = make_time(this_ts)
    else:
        time_dt = datetime.fromtimestamp(this_ts, tz=timezone.utc)
    if unit == '%M':
        temp = time_dt + timedelta(minutes=by_interval)
    else:
        raise NotImplementedError(f'Unexpected unit {unit}')
    return temp


def http_get(url, local_fqn):
    """Retrieve a file via http.

    :param url where the file can be found.
    :param local_fqn fully qualified name for where to file the file
        locally.
    """
    try:
        with requests.get(url, stream=True, timeout=10) as r:
            r.raise_for_status()
            with open(local_fqn, 'wb') as f:
                for chunk in r.iter_content(chunk_size=READ_BLOCK_SIZE):
                    f.write(chunk)
            length = to_int(r.headers.get('Content-Length'))
            if length is not None:
                file_meta = get_file_meta(local_fqn)
                if file_meta['size'] != length:
                    raise CadcException(
                        f'Could not retrieve {local_fqn} from {url}. File '
                        f'size error.'
                    )
            checksum = r.headers.get('Content-Checksum')
            if checksum is not None:
                file_meta = get_file_meta(local_fqn)
                if file_meta['md5sum'] != checksum:
                    raise CadcException(
                        f'Could not retrieve {local_fqn} from {url}. File '
                        f'checksum error.'
                    )
        if not os.path.exists(local_fqn):
            raise CadcException(
                f'Retrieve failed. {local_fqn} does not exist.'
            )
    except requests.exceptions.HTTPError as e:
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not retrieve {local_fqn} from {url}. Failed with {e}'
        )


@dataclass
class FileMeta:
    """The bits of information about a file that are used to decide whether
    or not to transfer, and whether or not a transfer was successful."""

    f_size: int
    md5sum: str


def _get_file_info(storage_name, cadc_client):
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


def query_tap(query_string, proxy_fqn, resource_id):
    """
    :param query_string ADQL
    :param proxy_fqn proxy file location, credentials for the query
    :param resource_id which tap service to query
    :returns an astropy votable instance."""

    logging.debug(
        f'query_tap: execute query {query_string} against {resource_id}'
    )
    subject = net.Subject(certificate=proxy_fqn)
    tap_client = CadcTapClient(subject, resource_id=resource_id)
    buffer = io.StringIO()
    tap_client.query(
        query_string, output_file=buffer, data_only=True, response_format='csv'
    )
    return Table.read(buffer.getvalue().split('\n'), format='csv')


def reverse_lookup(value_to_find, in_dict):
    """
    Use a generator expression to do a reverse-lookup in a dictionary.
    :param value_to_find value
    :param in_dict dictionary that might have a key for the value
    """
    exp = (key for key, value in in_dict.items() if value == value_to_find)
    result = None
    for entry in exp:
        result = entry
        break
    return result


def find_missing(compare_this, to_this):
    """
    :param compare_this: list
    :param to_this: list
    :return: list of elements from to_this not in compare_this
    """
    return list(set(compare_this).difference(to_this))


class ValueRepairCache(Cache):
    """Use a cache file to repair metadata values.

    The cache file is identified in the config.yml file.

    The cache file contents should look like:
    value_repair:  # must have this value
      observation.type:  # a fully-qualified caom2 entity.attribute value
        dark: DARK       # a key: value pair

    There can be multiple entries per attribute:
      e.g. observation.type:
             dark: DARK
             Dark: DARK
             object: OBJECT

    The key and the value are treated as regular expressions, with the
    following exceptions:

    Use the string value 'none' to set an attribute to None:
      e.g. observation.type:
             dark: none

    Use the string value 'none' to set un-set attributes to a value:
      e.g. observation.type:
             none: DARK

    Use the string value 'any' to set all values of an attribute to a single
    value:
      e.g. observation.type:
             any: DARK

    Caveats:

    Some CAOM2 attributes are not assignable. These attributes cannot be
    repaired using this class.

    List content cannot be repaired using this class.

    Values that are originally None are un-changed, unless 'none' is provided
    as the key.

    """

    VALUE_REPAIR = 'value_repair'

    def __init__(self):
        super().__init__()
        self._value_repair = self.get_from(ValueRepairCache.VALUE_REPAIR)
        self._key = None
        self._values = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def repair(self, observation):
        try:
            for key, values in self._value_repair.items():
                self._logger.debug(f'Checking for {key}')
                self._key = key
                self._values = values
                bits = key.split('.')[1:]
                if key.startswith('observation'):
                    self._recurse(observation, 'observation', bits)
                elif key.startswith('plane'):
                    for plane in observation.planes.values():
                        self._recurse(plane, 'plane', bits)
                elif key.startswith('artifact'):
                    for plane in observation.planes.values():
                        for artifact in plane.artifacts.values():
                            self._recurse(artifact, 'artifact', bits)
                elif key.startswith('part'):
                    for plane in observation.planes.values():
                        for artifact in plane.artifacts.values():
                            for part in artifact.parts.values():
                                self._recurse(part, 'part', bits)
                elif key.startswith('chunk'):
                    for plane in observation.planes.values():
                        for artifact in plane.artifacts.values():
                            for part in artifact.parts.values():
                                for chunk in part.chunks:
                                    self._recurse(chunk, 'chunk', bits)
                else:
                    raise CadcException(f'Unexpected repair key {key}.')
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            raise CadcException(e)

    def _recurse(self, entity, entity_name, bits):
        attribute_name = bits[0]
        if hasattr(entity, attribute_name):
            if len(bits) == 1:
                self._repair_attribute(entity, attribute_name)
            else:
                new_entity = getattr(entity, attribute_name)
                self._recurse(new_entity, attribute_name, bits[1:])
        else:
            logging.warning(
                f'No attribute {entity_name}.{attribute_name} found to repair.'
            )

    def _repair_attribute(self, entity, attribute_name):
        try:
            attribute_value = getattr(entity, attribute_name)
            if attribute_value not in self._values.values():
                for original, fix in self._values.items():
                    if attribute_value is None and original != 'none':
                        self._logger.info(
                            f'{attribute_name} value is None. This class only '
                            f'repairs values.'
                        )
                    else:
                        fixed = self._fix(
                            entity,
                            attribute_name,
                            attribute_value,
                            original,
                            fix,
                        )
                        if (
                            fixed is None
                            or fixed == fix
                            or fixed != attribute_value
                        ):
                            break
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            raise CadcException(e)

    def _fix(self, entity, attribute_name, attribute_value, original, fix):
        if fix == 'none':
            setattr(entity, attribute_name, None)
            self._logger.info(f'Repair {self._key} from {original} to None')
            fixed = None
        elif original == 'any':
            setattr(entity, attribute_name, fix)
            self._logger.info(
                f'Repair {self._key} from {attribute_value} to {fix}'
            )
            fixed = fix
        else:
            if attribute_value is None:
                setattr(entity, attribute_name, fix)
                fixed = fix
            else:
                attribute_value_type = type(attribute_value)
                fixed = re.sub(str(original), str(fix), str(attribute_value))
                setattr(entity, attribute_name, attribute_value_type(fixed))
        new_value = getattr(entity, attribute_name)
        if attribute_value != new_value:
            self._logger.info(
                f'Repair {self._key} from {attribute_value} to {fixed}'
            )
        return fixed

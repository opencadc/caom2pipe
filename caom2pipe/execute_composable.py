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

"""
This module contains pipeline execution classes. Each execution class
corresponds to a single task type, correlated with specific configuration
or implementation assumptions.

The execute methods in each of the class definitions require no if statements.
All the if statements are limited to the choose* methods in the
OrganizeExecutes class. If you find yourself adding an if statement to an
execute method, create a new *Execute class instead. The result is execute
methods that are composable into complex and varied pipelines, while
remaining easily tested. The execute methods do conform to an Airflow API
for operator extension, but please, please, please, do not ever import an
Airflow class here.

Alternatively, if you find yourself adding an if statement here, evaluate
whether the need could be solved with a collection-specific visit method
instead.

Raise the CadcException upon encountering an error. There is no recovery
effort as part of a failure. Log the error and stop the pipeline
execution for an Observation.

The correlations that currently exist:
- use_local_data: True => classes have "Local" in their name
- uses the CadcDataClient, the vos.Client, or the Caom2RepoClient => classes
    have "Client" in their name
- requires metadata access only => classes have "Meta" in their name
- requires data access => classes have "Data" in their name

What VISIT means when it comes to an CaomExecute specialization:
- visit does NOT execute the 'to_caom2' method
- *MetaVisit* executes all metadata visitors. These specializations leave
  the metadata retrieval to the metadata visitor. If the visitor
  implementation in a specific pipeline is careless, this may mean the VISIT
  task type can not stand on its own.
- *DataVisit* executes all data visitors as an implementation of the MODIFY
  task type. These specializations retrieve the data prior to executing the
  visitors.
*DataVisit and *MetaVisit specializations don't behave the same way, or have
the same assumptions about execution environment.

"""

import bz2
import gzip
import logging
import os
import traceback

from datetime import datetime
from shutil import copyfileobj, move
from urllib.parse import urlparse

from caom2utils.data_util import get_local_file_info
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc
from caom2pipe import transfer_composable as tc

__all__ = ['CaomExecute', 'OrganizeExecutes', 'OrganizeChooser']


class CaomExecute:
    """Abstract class that defines the operations common to all Execute
    classes."""

    def __init__(
        self,
        config,
        meta_visitors,
        observable,
        metadata_reader,
        clients,
    ):
        """
        :param config: Configurable parts of execution, as stored in
            manage_composable.Config.
        :param meta_visitors: List of classes with a
            'visit(observation, **kwargs)' method signature. Requires access
            to metadata only.
        :param observable: things that last longer than a pipeline execution
        :param metadata_reader: instance of MetadataReader, for retrieving
            metadata, for implementations that visit on metadata only.
        :param clients: instance of ClientCollection, for passing around
            long-lived https sessions, mostly
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(config.logging_level)
        formatter = logging.Formatter(
            '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s'
        )
        for handler in self._logger.handlers:
            handler.setLevel(config.logging_level)
            handler.setFormatter(formatter)
        (
            self.logging_level_param,
            self.log_level_as,
        ) = self._specify_logging_level_param(config.logging_level)
        self.root_dir = config.working_directory
        self._config = config
        self._working_dir = None
        self._model_fqn = None
        self._storage_name = None
        self._decompressor = None
        if clients is not None:
            self.cadc_client = clients.data_client
            self.caom_repo_client = clients.metadata_client
        self._clients = clients
        self.stream = None
        if hasattr(config, 'stream'):
            self.stream = (
                None
                if config.features.supports_latest_client
                else config.stream
            )
        self.meta_visitors = meta_visitors
        self.observable = observable
        self.log_file_directory = None
        self.data_visitors = []
        self.supports_latest_client = config.features.supports_latest_client
        self._metadata_reader = metadata_reader
        self._observation = None
        # track whether the caom2repo call will be a create or an update
        self._caom2_update_needed = False

    def __str__(self):
        return (
            f'\n'
            f'        obs_id: {self._storage_name.obs_id}\n'
            f'     model_fqn: {self._model_fqn}\n'
            f'   working_dir: {self._working_dir}\n'
        )

    @property
    def storage_name(self):
        return self._storage_name

    @storage_name.setter
    def storage_name(self, value):
        self._storage_name = value
        self._working_dir = os.path.join(self.root_dir, value.obs_id)
        if self._config.log_to_file:
            self._model_fqn = os.path.join(self._config.log_file_directory, value.model_file_name)
        else:
            self._model_fqn = os.path.join(self._working_dir, value.model_file_name)
        self._decompressor = decompressor_factory(
            self._config, self._working_dir, self.log_level_as, value
        )

    @property
    def working_dir(self):
        return self._working_dir

    def _caom2_read(self):
        """Retrieve the existing observation model metadata."""
        self._observation = clc.repo_get(
            self.caom_repo_client,
            self._storage_name.collection,
            self._storage_name.obs_id,
            self.observable.metrics,
        )
        self._caom2_update_needed = (
            False if self._observation is None else True
        )
        if self._caom2_update_needed:
            self._logger.debug(
                f'Found observation {self._observation.observation_id}'
            )

    def _caom2_store(self):
        """Update an existing observation instance.  Assumes the obs_id
        values are set correctly."""
        if self._caom2_update_needed:
            clc.repo_update(
                self.caom_repo_client,
                self._observation,
                self.observable.metrics,
            )
        else:
            clc.repo_create(
                self.caom_repo_client,
                self._observation,
                self.observable.metrics,
            )

    def _caom2_delete_create(self):
        """Delete an observation instance based on an input parameter."""
        if self._caom2_update_needed:
            clc.repo_delete(
                self.caom_repo_client,
                self._observation.collection,
                self._observation.observation_id,
                self.observable.metrics,
            )
        clc.repo_create(
            self.caom_repo_client,
            self._observation,
            self.observable.metrics,
        )

    def _cadc_put(self, source_fqn, uri):
        interim_fqn = self._decompressor.fix_compression(source_fqn)
        self.cadc_client.put(os.path.dirname(interim_fqn), uri, self.stream)
        # fix FileInfo that becomes out-dated by decompression during a STORE
        # task, in this common location, affecting all collections
        if source_fqn != interim_fqn:
            self._logger.debug(f'Recalculate FileInfo for {interim_fqn}')
            interim_file_info = get_local_file_info(interim_fqn)
            self._metadata_reader.file_info[uri] = interim_file_info

    def _read_model(self):
        """Read an observation into memory from an XML file on disk."""
        self._observation = None
        if os.path.exists(self._model_fqn):
            self._observation = mc.read_obs_from_file(self._model_fqn)
        self._caom2_update_needed = (
            False if self._observation is None else True
        )

    def _visit_meta(self):
        """Execute metadata-only visitors on an Observation in
        memory."""
        if self.meta_visitors is not None and len(self.meta_visitors) > 0:
            kwargs = {
                'working_directory': self._working_dir,
                'clients': self._clients,
                'stream': self.stream,
                'storage_name': self._storage_name,
                'metadata_reader': self._metadata_reader,
                'observable': self.observable,
            }
            for visitor in self.meta_visitors:
                try:
                    self._logger.debug(f'Visit for {visitor}')
                    self._observation = visitor.visit(
                        self._observation, **kwargs
                    )
                    if self._observation is None:
                        msg = (
                            f'Observation construction failed for '
                            f'{self._storage_name.file_uri}'
                        )
                        self._logger.error(
                            f'Stopping _visit_meta in {visitor.__name__} '
                            f'with {msg}'
                        )
                        raise mc.CadcException(msg)
                except Exception as e:
                    raise mc.CadcException(e)

    def _write_model(self):
        """Write an observation to disk from memory, represented in XML."""
        if self._observation is not None:
            self._logger.debug(f'Write model to {self._model_fqn}.')
            mc.write_obs_to_file(self._observation, self._model_fqn)

    def execute(self, context):
        self._logger.debug('Begin execute')
        self._logger.debug('the steps:')
        self.storage_name = context.get('storage_name')

    @staticmethod
    def _specify_logging_level_param(logging_level):
        """Make a configured logging level into command-line parameters."""
        lookup = {
            logging.DEBUG: ('--debug', logging.debug),
            logging.INFO: ('--verbose', logging.info),
            logging.WARNING: ('', logging.warning),
            logging.ERROR: ('--quiet', logging.error),
        }
        return lookup.get(logging_level, ('', logging.info))


class MetaVisitDeleteCreate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo delete followed by
    a create, because an update will not support a Simple->Derived
    or Derived->Simple type change for the Observation
    structure."""

    def __init__(
        self,
        config,
        meta_visitors,
        observable,
        metadata_reader,
        clients,
    ):
        super().__init__(
            config,
            meta_visitors,
            observable,
            metadata_reader,
            clients=clients,
        )

    def execute(self, context):
        super().execute(context)

        self._logger.debug('retrieve the observation if it exists')
        self._caom2_read()

        self._logger.debug('write the observation to disk for next step')
        self._write_model()

        self._logger.debug('the metadata visitors')
        self._visit_meta()

        self._logger.debug('write the observation to disk for debugging')
        self._write_model()

        self._logger.debug('the observation exists, delete it, then store it')
        self._caom2_delete_create()

        self._logger.debug('End execute')


class MetaVisit(CaomExecute):
    """
    Defines the pipeline step for Collection creation or augmentation by
    a visitor of metadata into CAOM.
    """

    def __init__(
        self,
        config,
        meta_visitors,
        observable,
        metadata_reader,
        clients,
    ):
        super().__init__(
            config,
            meta_visitors=meta_visitors,
            observable=observable,
            metadata_reader=metadata_reader,
            clients=clients,
        )

    def execute(self, context):
        super().execute(context)

        self._logger.debug('retrieve the observation if it exists')
        self._caom2_read()

        self._logger.debug('the metadata visitors')
        self._visit_meta()

        self._logger.debug('write the updated xml to disk for debugging')
        self._write_model()

        self._logger.debug('store the xml')
        self._caom2_store()

        self._logger.debug('End execute')


class DataVisit(CaomExecute):
    """Defines the pipeline step for all the operations that
    require access to the file on disk. The data must be retrieved
    from a separate source.

    :param transferrer: instance of transfer_composable.Transfer -
        how to get data from any DataSource for execution.
    """

    def __init__(
        self,
        config,
        data_visitors,
        observable,
        transferrer,
        clients,
        metadata_reader,
    ):
        super().__init__(
            config,
            meta_visitors=None,
            observable=observable,
            metadata_reader=metadata_reader,
            clients=clients,
        )
        self._data_visitors = data_visitors
        self._log_file_directory = config.log_file_directory
        self._transferrer = transferrer

    def execute(self, context):
        super().execute(context)

        self._logger.debug('get the input files')
        for entry in self._storage_name.destination_uris:
            local_fqn = os.path.join(
                self._working_dir, os.path.basename(entry)
            )
            self._transferrer.get(entry, local_fqn)

        self._logger.debug('get the observation for the existing model')
        self._caom2_read()

        self._logger.debug('execute the data visitors')
        self._visit_data()

        self._logger.debug('write the observation to disk for debugging')
        self._write_model()

        self._logger.debug('store the updated xml')
        self._caom2_store()

        self._logger.debug('End execute.')

    def _visit_data(self):
        """Execute the visitors that require access to the full data content
        of a file."""
        kwargs = {
            'working_directory': self._working_dir,
            'storage_name': self._storage_name,
            'log_file_directory': self._log_file_directory,
            'clients': self._clients,
            'stream': self.stream,
            'observable': self.observable,
            'metadata_reader': self._metadata_reader,
        }
        for visitor in self._data_visitors:
            try:
                self._logger.debug(f'Visit for {visitor.__class__.__name__}')
                self._observation = visitor.visit(self._observation, **kwargs)
            except Exception as e:
                raise mc.CadcException(e)


class LocalDataVisit(DataVisit):
    """Defines the pipeline step for all the operations that
    require access to the file on disk. This class assumes it has access to
    the files on disk - i.e. there is not need to retrieve the files from
    the CADC storage system, but there is a need to update CAOM
    entries with the service.
    """

    def __init__(
        self,
        config,
        data_visitors,
        observable,
        clients,
        metadata_reader,
    ):
        super().__init__(
            config,
            clients=clients,
            data_visitors=data_visitors,
            observable=observable,
            transferrer=tc.Transfer(),
            metadata_reader=metadata_reader,
        )

    def execute(self, context):
        self._logger.debug('Begin execute')
        self._logger.debug('the steps:')
        self.storage_name = context.get('storage_name')

        self._logger.debug('get the observation for the existing model')
        self._caom2_read()

        self._logger.debug('execute the data visitors')
        self._visit_data()

        self._logger.debug('write the updated xml to disk for debugging')
        self._write_model()

        self._logger.debug('store the updated xml')
        self._caom2_store()

        self._logger.debug(f'End execute')


class DataScrape(DataVisit):
    """Defines the pipeline step for Collection generation and ingestion of
    operations that require access to the file on disk, with no update to the
    service at the end. This class assumes it has access to the files on disk.
    The organization of this class assumes the 'Scrape' task has been done
    previously, so the model instance exists on disk.

    This executor requires manage_composable.Config.log_to_file to be True.
    """

    def __init__(self, config, data_visitors, observable, metadata_reader):
        super().__init__(
            config,
            clients=None,
            data_visitors=data_visitors,
            observable=observable,
            transferrer=tc.Transfer(),
            metadata_reader=metadata_reader,
        )

    def execute(self, context):
        self._logger.debug('Begin execute')
        self._logger.debug('the steps:')
        self.storage_name = context.get('storage_name')

        self._logger.debug('get observation for the existing model from disk')
        self._read_model()

        self._logger.debug('execute the data visitors')
        self._visit_data()

        self._logger.debug('output the updated xml')
        self._write_model()

        self._logger.debug('End execute')


class Store(CaomExecute):
    """Defines the pipeline step for Collection storage of a file. This
    requires access to the file on disk.

    Need the metadata_reader instance in case decompression occurs, resulting
    in a change to the FileInfo.
    """

    def __init__(
        self,
        config,
        observable,
        transferrer,
        clients,
        metadata_reader,
    ):
        super().__init__(
            config,
            meta_visitors=None,
            observable=observable,
            metadata_reader=metadata_reader,
            clients=clients,
        )
        self._transferrer = transferrer

    def execute(self, context):
        super().execute(context)

        self._logger.debug(
            f'Store {len(self._storage_name.source_names)} files to CADC.'
        )
        for index, entry in enumerate(self._storage_name.source_names):
            temp = urlparse(entry)
            local_fqn = os.path.join(
                self._working_dir, temp.path.split('/')[-1]
            )
            self._logger.debug(f'Retrieve {entry} to {local_fqn}')
            self._transferrer.get(entry, local_fqn)

            self._logger.debug(
                f'store the input file {local_fqn} to '
                f'{self._storage_name.destination_uris[index]}'
            )
            self._cadc_put(
                local_fqn, self._storage_name.destination_uris[index]
            )
            self._transferrer.post_store_check(
                entry, self._storage_name.destination_uris[index]
            )

        self._logger.debug('End execute')


class LocalStore(Store):
    """Defines the pipeline step for Collection storage of a file. This
    requires access to the file on disk. The file originates from local
    disk.

    Need the metadata_reader instance in case decompression occurs, resulting
    in a change to the FileInfo.
    """

    def __init__(
        self,
        config,
        observable,
        clients,
        metadata_reader,
    ):
        super().__init__(
            config,
            observable,
            transferrer=None,
            clients=clients,
            metadata_reader=metadata_reader,
        )

    def execute(self, context):
        self._logger.debug('Begin execute')
        self._logger.debug('the steps:')
        self.storage_name = context.get('storage_name')

        self._logger.debug(
            f'Store {len(self._storage_name.source_names)} files to CADC.'
        )
        for index, entry in enumerate(self._storage_name.source_names):
            self._logger.debug(f'store the input file {entry}')
            self._cadc_put(entry, self._storage_name.destination_uris[index])

        self._logger.debug('End execute')


class Scrape(CaomExecute):
    """Defines the pipeline step for Collection creation of a CAOM model
    observation. The file containing the metadata is located on disk.
    No record is written to a web service."""

    def __init__(
        self,
        config,
        observable,
        meta_visitors,
        metadata_reader,
    ):
        super().__init__(
            config,
            meta_visitors=meta_visitors,
            observable=observable,
            metadata_reader=metadata_reader,
            clients=None,
        )

    def execute(self, context):
        super().execute(context)

        self._logger.debug('get observation for the existing model from disk')
        self._read_model()

        self._logger.debug('the metadata visitors')
        self._visit_meta()

        self._logger.debug('write the updated xml to disk for debugging')
        self._write_model()

        self._logger.debug(f'End execute')


class OrganizeChooser:
    """Extend this class to provide a way to make collection-specific
    complex conditions available within the OrganizeExecute class."""

    def __init__(self):
        pass

    def needs_delete(self):
        return False

    def use_compressed(self, f=None):
        return False


class OrganizeExecutes:
    """How to turn on/off various task types in a CaomExecute pipeline."""

    def __init__(
        self,
        config,
        meta_visitors,
        data_visitors,
        chooser=None,
        store_transfer=None,
        modify_transfer=None,
        metadata_reader=None,
        clients=None,
    ):
        """
        Why there is support for two transfer instances:
        - the store_transfer instance may do an http, ftp, or vo transfer
            from an external source, for the purposes of storage
        - the modify_transfer instance probably does a CADC retrieval,
            so that metadata production that relies on the data content
            (e.g. preview generation) can occur

        :param config:
        :param meta_visitors List of metadata visit methods.
        :param data_visitors List of data visit methods.
        :param chooser:
        :param store_transfer Transfer implementation for retrieving files
        :param modify_transfer Transfer implementation for retrieving files
        :param clients ClientCollection instance
        :param metadata_reader client instance for reading headers,
            passed on to to_caom2_client.
        """
        self.config = config
        self.chooser = chooser
        self.task_types = config.task_types
        self.todo_fqn = None
        self.success_fqn = None
        self.failure_fqn = None
        self.retry_fqn = None
        self.rejected_fqn = None
        self.set_log_location()
        self._success_count = 0
        self._rejected_count = 0
        self._complete_record_count = 0
        self._timeout = 0
        self.observable = mc.Observable(
            mc.Rejected(self.rejected_fqn), mc.Metrics(config)
        )
        self._meta_visitors = meta_visitors
        self._data_visitors = data_visitors
        self._modify_transfer = modify_transfer
        self._store_transfer = store_transfer
        # use the same Observable everywhere
        if modify_transfer is not None:
            self._modify_transfer.observable = self.observable
        if store_transfer is not None:
            self._store_transfer.observable = self.observable
        if clients is not None:
            clients.metrics = self.observable.metrics
            self._cadc_client = clients.data_client
            self._caom_client = clients.metadata_client
        self._clients = clients
        self._metadata_reader = metadata_reader
        self._log_h = None
        self._executors = []
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(config.logging_level)

    def _clean_up_workspace(self, obs_id):
        """Remove a directory and all its contents. Only do this if there
        is not a 'SCRAPE' task type, since the point of scraping is to
        be able to look at the pipeline execution artefacts once the
        processing is done.
        """
        working_dir = os.path.join(self.config.working_directory, obs_id)
        if (
            os.path.exists(working_dir)
            and mc.TaskType.SCRAPE not in self.config.task_types
        ):
            for ii in os.listdir(working_dir):
                os.remove(os.path.join(working_dir, ii))
            os.rmdir(working_dir)
        self._logger.debug(
            f'Removed working directory {working_dir} and contents.'
        )

    def _create_workspace(self, obs_id):
        """Create the working area if it does not already exist."""
        working_dir = os.path.join(self.config.working_directory, obs_id)
        self._logger.debug(f'Create working directory {working_dir}')
        mc.create_dir(working_dir)

    def set_log_files(self, config):
        self.todo_fqn = config.work_fqn
        self.success_fqn = config.success_fqn
        self.failure_fqn = config.failure_fqn
        self.retry_fqn = config.retry_fqn
        self.rejected_fqn = config.rejected_fqn

    @property
    def rejected_count(self):
        """:return integer indicating how many inputs (files or observations,
        depending on the configuration) have been rejected for well-known
        reasons."""
        return self._rejected_count

    @property
    def success_count(self):
        """:return integer indicating how many inputs (files or observations,
        depending on the configuration) have been successfully processed."""
        return self._success_count

    @success_count.setter
    def success_count(self, value):
        self._success_count = value

    @property
    def complete_record_count(self):
        """:return integer indicating how many inputs (files or observations,
        depending on the configuration) have been processed."""
        return self._complete_record_count

    @complete_record_count.setter
    def complete_record_count(self, value):
        self._complete_record_count = value

    @property
    def timeouts(self):
        return self._timeout

    def capture_failure(self, storage_name, e, stack_trace):
        """Log an error message to the failure file.

        If the failure is of a known type, also capture it to the rejected
        list. The rejected list will be saved to disk when the execute method
        completes.

        :obs_id observation ID being processed
        :file_name file name being processed
        :e Exception to log - the entire stack trace, which, if logging
            level is not set to debug, will be lost for debugging purposes.
        """
        self._count_timeouts(stack_trace)
        with open(self.failure_fqn, 'a') as failure:
            if e.args is not None and len(e.args) > 1:
                min_error = e.args[0]
            else:
                min_error = str(e)
            failure.write(
                f'{datetime.now()} {storage_name.obs_id} '
                f'{storage_name.file_name} {min_error}\n'
            )

        # only retry entries that are not permanently marked as rejected
        reason = mc.Rejected.known_failure(stack_trace)
        if reason == mc.Rejected.NO_REASON:
            with open(self.retry_fqn, 'a') as retry:
                for entry in storage_name.source_names:
                    retry.write(f'{entry}\n')
        else:
            self.observable.rejected.record(reason, storage_name.obs_id)
            self._rejected_count += 1

    def capture_success(self, obs_id, file_name, start_time):
        """Capture, with a timestamp, the successful observations/file names
        that have been processed.
        :obs_id observation ID being processed
        :file_name file name being processed
        :start_time seconds since beginning of execution.
        """
        self.success_count += 1
        execution_s = datetime.utcnow().timestamp() - start_time
        success = open(self.success_fqn, 'a')
        try:
            success.write(
                f'{datetime.now()} {obs_id} {file_name} '
                f'{execution_s:.2f}\n'
            )
        finally:
            success.close()
        msg = (
            f'Progress - record {self.success_count} of '
            f'{self.complete_record_count} records processed in '
            f'{execution_s:.2f} s.'
        )
        self._logger.debug('*' * len(msg))
        self._logger.info(msg)
        self._logger.debug('*' * len(msg))

    def set_log_location(self):
        self.set_log_files(self.config)
        mc.create_dir(self.config.log_file_directory)
        now_s = datetime.utcnow().timestamp()
        for fqn in [self.success_fqn, self.failure_fqn, self.retry_fqn]:
            OrganizeExecutes.init_log_file(fqn, now_s)
        self._success_count = 0

    def is_rejected(self, storage_name):
        """Common code to use the appropriate identifier when checking for
        rejected entries."""
        result = self.observable.rejected.is_bad_metadata(
            storage_name.file_name,
        )
        if result:
            self._logger.info(
                f'Rejected observation {storage_name.file_name} because of '
                f'bad metadata'
            )
        return result

    @staticmethod
    def init_log_file(log_fqn, now_s):
        """Keep old versions of the progress files."""
        log_fid = log_fqn.replace('.txt', '')
        back_fqn = f'{log_fid}.{now_s}.txt'
        if os.path.exists(log_fqn) and os.path.getsize(log_fqn) != 0:
            move(log_fqn, back_fqn)
        f_handle = open(log_fqn, 'w')
        f_handle.close()

    def _count_timeouts(self, e):
        if e is not None and (
            'Read timed out' in e
            or 'reset by peer' in e
            or 'ConnectTimeoutError' in e
            or 'Broken pipe' in e
        ):
            self._timeout += 1

    def _set_up_file_logging(self, storage_name):
        """Configure logging to a separate file for each entry being
        processed.

        If log_to_file is set to False, don't create a separate log file for
        each entry, because the application should leave as small a logging
        trace as possible.

        """
        if self.config.log_to_file:
            log_fqn = os.path.join(
                self.config.working_directory, storage_name.log_file
            )
            if self.config.log_file_directory is not None:
                log_fqn = os.path.join(
                    self.config.log_file_directory, storage_name.log_file
                )
            self._log_h = logging.FileHandler(log_fqn)
            formatter = logging.Formatter(
                '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s'
            )
            self._log_h.setLevel(self.config.logging_level)
            self._log_h.setFormatter(formatter)
            logging.getLogger().addHandler(self._log_h)

    def _unset_file_logging(self):
        """Turn off the logging to the separate file for each entry being
        processed."""
        if self.config.log_to_file:
            logging.getLogger().removeHandler(self._log_h)
            self._log_h.flush()
            self._log_h.close()

    def choose(self):
        """The logic that decides which descendants of CaomExecute to
        instantiate. This is based on the content of the config.yml file
        for an application.
        :destination_name StorageName extension that handles the naming rules
            for a file.
        """
        if mc.TaskType.SCRAPE not in self.task_types:
            for entry in [self._modify_transfer, self._store_transfer]:
                if entry is not None:
                    # set only for Transfer specializations that have a
                    # cadc_client attribute (HttpTransfer, FtpTransfer do not)
                    if hasattr(entry, '_cadc_client'):
                        entry.cadc_client = self._cadc_client
        for task_type in self.task_types:
            if task_type == mc.TaskType.SCRAPE:
                if self.config.use_local_files:
                    self._logger.debug(
                        f'Choosing executor Scrape for {task_type}.'
                    )
                    self._executors.append(
                        Scrape(
                            self.config,
                            self.observable,
                            self._meta_visitors,
                            self._metadata_reader,
                        )
                    )

                else:
                    raise mc.CadcException(
                        'use_local_files must be True with Task Type '
                        '"SCRAPE"'
                    )
            elif task_type == mc.TaskType.STORE:
                if self.config.use_local_files:
                    self._logger.debug(
                        f'Choosing executor LocalStore for {task_type}.'
                    )
                    self._executors.append(
                        LocalStore(
                            self.config,
                            self.observable,
                            self._clients,
                            self._metadata_reader,
                        )
                    )
                else:
                    self._logger.debug(
                        f'Choosing executor Store for {task_type}.'
                    )
                    self._executors.append(
                        Store(
                            self.config,
                            self.observable,
                            self._store_transfer,
                            self._clients,
                            self._metadata_reader,
                        )
                    )
            elif task_type == mc.TaskType.INGEST:
                if self.chooser is not None and self.chooser.needs_delete():
                    self._logger.debug(
                        f'Choosing executor MetaVisitDeleteCreate for '
                        f'{task_type}.'
                    )
                    self._executors.append(
                        MetaVisitDeleteCreate(
                            self.config,
                            self._meta_visitors,
                            self.observable,
                            self._metadata_reader,
                            self._clients,
                        )
                    )
                else:
                    self._logger.debug(
                        f'Choosing executor MetaVisit for {task_type}.'
                    )
                    self._executors.append(
                        MetaVisit(
                            self.config,
                            self._meta_visitors,
                            self.observable,
                            self._metadata_reader,
                            self._clients,
                        )
                    )
            elif task_type == mc.TaskType.MODIFY:
                if self.config.use_local_files:
                    if len(self._executors) > 0 and isinstance(self._executors[0], Scrape):
                        self._logger.debug(
                            f'Choosing executor DataScrape for '
                            f'{task_type}.'
                        )
                        self._executors.append(
                            DataScrape(
                                self.config,
                                self._data_visitors,
                                self.observable,
                                self._metadata_reader,
                            )
                        )
                    else:
                        self._logger.debug(
                            f'Choosing executor LocalDataVisit for '
                            f'{task_type}.'
                        )
                        self._executors.append(
                            LocalDataVisit(
                                self.config,
                                self._data_visitors,
                                self.observable,
                                self._clients,
                                self._metadata_reader,
                            )
                        )
                else:
                    self._logger.debug(
                        f'Choosing executor DataVisit for {task_type}.'
                    )
                    self._executors.append(
                        DataVisit(
                            self.config,
                            self._data_visitors,
                            self.observable,
                            self._modify_transfer,
                            self._clients,
                            self._metadata_reader,
                        )
                    )
            elif task_type == mc.TaskType.VISIT:
                self._logger.debug(
                    f'Choosing executor MetaVisit for {task_type}.'
                )
                self._executors.append(
                    MetaVisit(
                        self.config,
                        self._meta_visitors,
                        self.observable,
                        self._metadata_reader,
                        self._clients,
                    )
                )
            elif task_type == mc.TaskType.DEFAULT:
                pass
            else:
                raise mc.CadcException(
                    f'Do not understand task type {task_type}'
                )

    def do_one(self, storage_name):
        """Process one entry.
        :param storage_name instance of StorageName for the collection
        """
        self._logger.debug(f'Begin do_one {storage_name}')
        self._set_up_file_logging(storage_name)
        start_s = datetime.utcnow().timestamp()
        try:
            if self.is_rejected(storage_name):
                self.capture_failure(
                    storage_name,
                    BaseException('StorageName.is_rejected'),
                    'Rejected',
                )
                # successful rejection of the execution case
                result = 0
            else:
                self._metadata_reader.set(storage_name)
                self._create_workspace(storage_name.obs_id)
                context = {'storage_name': storage_name}
                for executor in self._executors:
                    self._logger.info(
                        f'Task with {executor.__class__.__name__} for '
                        f'{storage_name.obs_id}'
                    )
                    executor.execute(context)
                if len(self._executors) > 0:
                    self.capture_success(
                        storage_name.obs_id, storage_name.file_name, start_s
                    )
                    result = 0
                else:
                    self._logger.info(f'No executors for {storage_name}')
                    result = -1  # cover case where file name validation fails
        except Exception as e:
            self.capture_failure(storage_name, e, traceback.format_exc())
            self._logger.warning(
                f'Execution failed for {storage_name.obs_id} with {e}'
            )
            self._logger.debug(traceback.format_exc())
            result = -1
        finally:
            self._clean_up_workspace(storage_name.obs_id)
            self._unset_file_logging()
        return result


def decompressor_factory(
    config, working_directory, log_level_as, storage_name
):
    if config.features.supports_decompression:
        if config.collection == 'CFHT':
            return FitsForCADCCompressor(
                working_directory, log_level_as, storage_name
            )
        else:
            return FitsForCADCDecompressor(working_directory, log_level_as)
    else:
        return DecompressorNoop()


class DecompressorNoop:
    def __init__(self):
        pass

    def fix_compression(self, fqn):
        return fqn


class FitsForCADCDecompressor(DecompressorNoop):
    """
    This class ensures that files stored at CADC are uncompressed if arriving
    with .gz or .bz2 compression.

    CADC storage is object-based. CADC offers cut-out services for FITS files.
    For this implementation choice and service offering to co-exist, any FITS
    file stored at CADC can only be compressed using something like fpack.

    JJK - 18-02-22
    Uncompress all the non-.fz compressed FITS files at CADC.
    SGw - 15-03-22
    Re-compress from gz=>fz only CFHT FITS files, because fpack/imcopy adds
    HDUs.
    """

    def __init__(self, working_directory, log_level_as):
        self._working_directory = working_directory
        self._log_level_as = log_level_as
        self._logger = logging.getLogger(self.__class__.__name__)

    def fix_compression(self, fqn):
        self._logger.debug(f'Begin fix_compression with {fqn}')
        returned_fqn = fqn
        if '.fits' in fqn:
            # if the decompressed file is put in the working directory for
            # the *Execute work, it should be cleaned up just like any other
            # temporary files, and it should also not interfere with a
            # "cleanup_files_when_storing" config
            if fqn.endswith('.gz'):
                returned_fqn = os.path.join(
                    self._working_directory,
                    os.path.basename(fqn).replace('.gz', ''),
                )
                self._logger.info(
                    f'Decompressing {fqn} with gunzip to {returned_fqn}'
                )
                with gzip.open(fqn, 'rb') as f_in, open(
                    returned_fqn, 'wb'
                ) as f_out:
                    # use shutil to control memory consumption
                    copyfileobj(f_in, f_out)
            elif fqn.endswith('.bz2'):
                returned_fqn = os.path.join(
                    self._working_directory,
                    os.path.basename(fqn).replace('.bz2', ''),
                )
                self._logger.info(
                    f'Decompressing {fqn} with bz2 to {returned_fqn}'
                )
                with open(returned_fqn, 'wb') as f_out, bz2.BZ2File(
                    fqn, 'rb'
                ) as f_in:
                    # use shutil to control memory consumption
                    copyfileobj(f_in, f_out)
        self._logger.debug(f'End fix_compression with {returned_fqn}')
        return returned_fqn


class FitsForCADCCompressor(FitsForCADCDecompressor):
    """
    This class implements conditional recompression.

    SF - 20-05-22
    it looks like compression will be:
    if bitpix==(-32|-64):
         gunzip file.fits.gz
    else:
         imcopy file.fits.gz file.fits.fz[compress]
    """

    def __init__(self, working_directory, log_level_as, storage_name):
        super().__init__(working_directory, log_level_as)
        self._storage_name = storage_name

    def fix_compression(self, fqn):
        self._logger.debug(f'Begin fix_compression with {fqn}')
        returned_fqn = fqn
        if '.fits' in fqn and fqn.endswith('.gz'):
            if self._storage_name.file_uri.endswith('.fz'):
                fz_fqn = os.path.join(
                    self._working_directory,
                    os.path.basename(fqn).replace('.gz', '.fz'),
                )
                compress_cmd = f"imcopy {fqn} '{fz_fqn}[compress]'"
                self._logger.debug(f'Executing {compress_cmd}')
                mc.exec_cmd_array(
                    ['/bin/bash', '-c', compress_cmd], self._log_level_as
                )
                self._logger.info(
                    f'Changed compressed file from {fqn} to {fz_fqn}'
                )
                returned_fqn = fz_fqn
                self._logger.debug(f'End fix_compression with {returned_fqn}')
            else:
                returned_fqn = super().fix_compression(fqn)
                # log message in the super
        return returned_fqn

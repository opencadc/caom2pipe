# -*- coding: utf-8 -*-
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

import distutils.sysconfig
import logging
import os
import sys
import traceback

from datetime import datetime
from dateutil import tz
from shutil import move

from cadcdata import CadcDataClient
from caom2repo import CAOM2RepoClient
from vos import Client
from caom2pipe import manage_composable as mc
from caom2pipe import transfer_composable as tc

__all__ = ['CaomExecute', 'OrganizeExecutes', 'OrganizeChooser']


class CaomExecute(object):
    """Abstract class that defines the operations common to all Execute
    classes."""

    def __init__(self, config, task_type, storage_name, command_name,
                 cred_param, cadc_client, caom_repo_client,
                 meta_visitors, observable):
        """
        :param config: Configurable parts of execution, as stored in
            manage_composable.Config.
        :param task_type: manage_composable.TaskType enumeration - identifies
            the work to do, in words that are user-facing. Used in logging
            messages.
        :param storage_name: An instance of StorageName.
        :param command_name: The collection-specific application to apply a
            blueprint. May be 'fits2caom2'.
        :param cred_param: either --netrc <value> or --cert <value>,
            depending on which credentials have been supplied to the
            process.
        :param cadc_client: Instance of CadcDataClient or Client. Used for
            CADC storage service access.
        :param caom_repo_client: Instance of CAOM2Repo client. Used for
            caom2 repository service access.
        :param meta_visitors: List of classes with a
            'visit(observation, **kwargs)' method signature. Requires access
            to metadata only.
        :param observable: things that last longer than a pipeline execution
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(config.logging_level)
        formatter = logging.Formatter(
            '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s')
        for handler in self.logger.handlers:
            handler.setLevel(config.logging_level)
            handler.setFormatter(formatter)
        self.logging_level_param, self.log_level_as = \
            self._specify_logging_level_param(config.logging_level)
        self.obs_id = storage_name.obs_id
        self.uri = storage_name.file_uri
        self.fname = storage_name.file_name
        self.command_name = command_name
        self.root_dir = config.working_directory
        self.collection = config.collection
        self.archive = config.archive
        if config.use_local_files:
            self.working_dir = self.root_dir
        else:
            self.working_dir = os.path.join(self.root_dir, self.obs_id)

        if config.log_to_file:
            self.model_fqn = os.path.join(config.log_file_directory,
                                          storage_name.model_file_name)
        else:
            self.model_fqn = os.path.join(self.working_dir,
                                          storage_name.model_file_name)
        self.resource_id = config.resource_id
        self.cadc_client = cadc_client
        self.caom_repo_client = caom_repo_client
        self.stream = config.stream
        self.meta_visitors = meta_visitors
        self.task_type = task_type
        self.cred_param = cred_param
        self.url = storage_name.url
        self.lineage = storage_name.lineage
        self.external_urls_param = self._specify_external_urls_param(
            storage_name.external_urls)
        self.observable = observable
        self.mime_encoding = storage_name.mime_encoding
        self.mime_type = storage_name.mime_type
        self._storage_name = storage_name
        self._fqn = None
        if self.working_dir and self.fname:
            self._fqn = os.path.join(self.working_dir, self.fname)
        self.log_file_directory = None
        self.data_visitors = []
        self.supports_latest_client = config.features.supports_latest_client
        if hasattr(config, 'store_newer_files_only'):
            self.store_newer_files_only = (config.store_newer_files_only and
                                           config.use_local_files)
        else:
            # do nothing different, if flag is missing from config
            self.store_newer_files_only = False

    def _cleanup(self):
        """Remove a directory and all its contents."""
        if os.path.exists(self.working_dir):
            for ii in os.listdir(self.working_dir):
                os.remove(os.path.join(self.working_dir, ii))
            os.rmdir(self.working_dir)

    def _create_dir(self):
        """Create the working area if it does not already exist."""
        mc.create_dir(self.working_dir)

    def _define_local_dirs(self, storage_name):
        """when files are on disk don't worry about a separate directory
        per observation"""
        self.working_dir = self.root_dir
        self.model_fqn = os.path.join(self.working_dir,
                                      storage_name.model_file_name)

    def _find_fits2caom2_plugin(self):
        """Find the code that is passed as the --plugin parameter to
        fits2caom2.

        This code makes the assumption that execution always occurs within
        the context of a Docker container, and therefore the
        get_python_lib call will always have the appropriately-named
        module installed in a site package location.
        """
        packages = distutils.sysconfig.get_python_lib()
        return os.path.join(
            packages, f'{self.command_name}/{self.command_name}.py')

    def _fits2caom2_cmd_local(self, connected=True):
        """
        Execute fits2caom with a --cert parameter and a --local parameter.
        """
        plugin = self._find_fits2caom2_plugin()
        command = mc.load_module(plugin, 'to_caom2')
        # so far, the plugin is also the module :)
        conn = ''
        if not connected:
            conn = f'--not_connected'
        local_fqn = os.path.join(self.working_dir, self.fname)
        sys.argv = (f'{self.command_name} {self.logging_level_param} {conn} '
                    f'{self.cred_param} --observation {self.collection} '
                    f'{self.obs_id} --local {local_fqn} --out '
                    f'{self.model_fqn} --plugin {plugin} --module {plugin} '
                    f'--lineage {self.lineage}').split()
        result = command.to_caom2()
        if result == -1:
            raise mc.CadcException(f'Error executing to_caom2 with {sys.argv}')

    def _fits2caom2_cmd(self):
        """Execute fits2caom with a --cert parameter."""
        plugin = self._find_fits2caom2_plugin()
        # so far, the plugin is also the module :)
        command = mc.load_module(plugin, 'to_caom2')
        sys.argv = (f'{self.command_name} {self.logging_level_param} '
                    f'{self.cred_param} --observation {self.collection} '
                    f'{self.obs_id} --out {self.model_fqn} '
                    f'{self.external_urls_param} --plugin {plugin} --module '
                    f'{plugin} --lineage {self.lineage}').split()
        result = command.to_caom2()
        if result == -1:
            raise mc.CadcException(f'Error executing to_caom2 with {sys.argv}')

    def _fits2caom2_cmd_in_out(self):
        """Execute fits2caom with a --in, a --external_url and a --cert
        parameter."""
        plugin = self._find_fits2caom2_plugin()
        # so far, the plugin is also the module :)
        command = mc.load_module(plugin, 'to_caom2')
        sys.argv = (f'{self.command_name} {self.logging_level_param} '
                    f'{self.cred_param} --in {self.model_fqn} --out '
                    f'{self.model_fqn} {self.external_urls_param} --plugin '
                    f'{plugin} --module {plugin} --lineage '
                    f'{self.lineage}').split()
        result = command.to_caom2()
        if result == -1:
            raise mc.CadcException(f'Error executing to_caom2 with {sys.argv}')

    def _fits2caom2_cmd_in_out_local(self, connected=True):
        """Execute fits2caom with a --in, --local and a --cert parameter."""
        plugin = self._find_fits2caom2_plugin()
        # so far, the plugin is also the module :)
        command = mc.load_module(plugin, 'to_caom2')
        local_fqn = os.path.join(self.working_dir, self.fname)
        conn = ''
        if not connected:
            conn = f'--not_connected'
        sys.argv = (f'{self.command_name} {self.logging_level_param} {conn} '
                    f'{self.cred_param} --in {self.model_fqn} --out '
                    f'{self.model_fqn} --local ' f'{local_fqn} --plugin '
                    f'{plugin} --module {plugin} --lineage '
                    f'{self.lineage}').split()
        result = command.to_caom2()
        if result == -1:
            raise mc.CadcException(f'Error executing to_caom2 with {sys.argv}')

    def _repo_cmd_create_client(self, observation):
        """Create an observation instance from the input parameter."""
        mc.repo_create(self.caom_repo_client, observation,
                       self.observable.metrics)

    def _repo_cmd_update_client(self, observation):
        """Update an existing observation instance.  Assumes the obs_id
        values are set correctly."""
        mc.repo_update(self.caom_repo_client, observation,
                       self.observable.metrics)

    def _repo_cmd_read_client(self):
        """Retrieve the existing observation model metadata."""
        return mc.repo_get(self.caom_repo_client, self.collection,
                           self.obs_id, self.observable.metrics)

    def _repo_cmd_delete_client(self, observation):
        """Delete an observation instance based on an input parameter."""
        mc.repo_delete(self.caom_repo_client, observation.collection,
                       observation.observation_id, self.observable.metrics)

    def _cadc_put(self, storage_name):
        """
        :param storage_name: Artifact URI
        """
        # if use_local_files is True, and store_newer_files_only is True,
        # and a file already exists at CADC, the file will only be sent for
        # storage if it's last modified time is later than the last modified
        # time of the file already at CADC
        transfer_data = True
        if self.store_newer_files_only:
            # get the metadata locally
            fqn = os.path.join(self.working_dir, self.fname)
            s = os.stat(fqn)
            local_timestamp = s.st_mtime
            # running from a Docker container with timezone UTC
            local_utc = datetime.fromtimestamp(local_timestamp, tz=tz.UTC)

            # get the metadata at CADC
            if self.supports_latest_client:
                cadc_meta = self.cadc_client.get_node(
                    storage_name, limit=None, force=False)
                cadc_timestamp = cadc_meta.props.get('date')
            else:
                cadc_meta = mc.get_cadc_meta_client(
                    self.cadc_client, self.archive, self.fname)
                cadc_timestamp = cadc_meta.get('lastmod')

            # CADC lastmod value looks like:
            # 'lastmod': 'Fri, 28 Feb 2020 05:04:41 GMT'
            cadc_utc = mc.make_time_tz(cadc_timestamp)
            if local_utc > cadc_utc:
                self.logger.debug(f'Transferring. {self.fname} has CADC '
                                  f'timestamp {cadc_utc}.')
            else:
                self.logger.warning(
                    f'{self.fname} newer at CADC. Not transferring.')
                transfer_data = False

        if transfer_data:
            if self.supports_latest_client:
                self._client_put(storage_name)
            else:
                self._cadc_data_put_client()

    def _cadc_data_put_client(self):
        """Store a collection file."""
        mc.data_put(self.cadc_client, self.working_dir,
                    self.fname, self.archive, self.stream, self.mime_type,
                    self.mime_encoding, metrics=self.observable.metrics)

    def _client_put(self, storage_name):
        """Store a collection file using VOS."""
        mc.client_put(self.cadc_client, self.working_dir,
                      self.fname, storage_name, self.observable.metrics)

    def _read_model(self):
        """Read an observation into memory from an XML file on disk."""
        return mc.read_obs_from_file(self.model_fqn)

    def _visit_meta(self, observation):
        """Execute metadata-only visitors on an Observation in
        memory."""
        if self.meta_visitors is not None and len(self.meta_visitors) > 0:
            kwargs = {'working_directory': self.working_dir,
                      'cadc_client': self.cadc_client,
                      'stream': self.stream,
                      'url': self.url,
                      'observable': self.observable}
            for visitor in self.meta_visitors:
                try:
                    self.logger.debug(f'Visit for {visitor}')
                    visitor.visit(observation, **kwargs)
                except Exception as e:
                    raise mc.CadcException(e)

    def _write_model(self, observation):
        """Write an observation to disk from memory, represented in XML."""
        mc.write_obs_to_file(observation, self.model_fqn)

    @staticmethod
    def _specify_external_urls_param(external_urls):
        """Make a list of external urls into a command-line parameter."""
        if external_urls is None:
            result = ''
        else:
            result = f'--external_url {external_urls}'
        return result

    @staticmethod
    def _specify_logging_level_param(logging_level):
        """Make a configured logging level into command-line parameters."""
        lookup = {logging.DEBUG: ('--debug', logging.debug),
                  logging.INFO: ('--verbose', logging.info),
                  logging.WARNING: ('', logging.warning),
                  logging.ERROR: ('--quiet', logging.error)}
        return lookup.get(logging_level, ('', logging.info))

    @staticmethod
    def repo_cmd_get_client(caom_repo_client, collection, observation_id,
                            metrics):
        """Execute the CAOM2Repo 'read' operation using the client instance
        from this class.
        :return an Observation instance, or None, if the observation id
        does not exist."""
        logging.error(caom_repo_client)
        return mc.repo_get(caom_repo_client, collection, observation_id,
                           metrics)


class MetaCreate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo create."""

    def __init__(self, config, storage_name, command_name,
                 cred_param, cadc_client, caom_repo_client,
                 meta_visitors, observable):
        super(MetaCreate, self).__init__(
            config, mc.TaskType.INGEST, storage_name, command_name,
            cred_param, cadc_client, caom_repo_client, meta_visitors,
            observable)
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('the observation does not exist, so go '
                          'straight to generating the xml, as the main_app '
                          'will retrieve the headers')
        self._fits2caom2_cmd()

        self.logger.debug('read the xml into memory from the file')
        observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(observation)

        self.logger.debug('write the observation to disk for debugging')
        self._write_model(observation)

        self.logger.debug('create the observation with xml')
        self._repo_cmd_create_client(observation)

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute')


class MetaUpdate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo update."""

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client, observation,
                 meta_visitors, observable):
        super(MetaUpdate, self).__init__(
            config, mc.TaskType.INGEST, storage_name, command_name, cred_param,
            cadc_client, caom_repo_client, meta_visitors, observable)
        self.observation = observation
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('write the observation to disk for next step')
        self._write_model(self.observation)

        self.logger.debug('generate the xml, as the main_app will retrieve '
                          'the headers')
        self._fits2caom2_cmd_in_out()

        self.logger.debug('read the xml from disk')
        self.observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(self.observation)

        self.logger.debug('write the observation to disk for next step')
        self._write_model(self.observation)

        self.logger.debug('update the observation with xml')
        self._repo_cmd_update_client(self.observation)

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute')


class MetaDeleteCreate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo delete followed by
    a create, because an update will not support a Simple->Composite
    or Composite->Simple type change for the Observation
    structure."""

    def __init__(self, config, storage_name, command_name,
                 cred_param, cadc_client, caom_repo_client,
                 observation, meta_visitors, observable):
        super(MetaDeleteCreate, self).__init__(
            config, mc.TaskType.INGEST, storage_name, command_name,
            cred_param, cadc_client, caom_repo_client, meta_visitors,
            observable)
        self.observation = observation
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('write the observation to disk for next step')
        self._write_model(self.observation)

        self.logger.debug('make a new observation from an existing '
                          'observation')
        self._fits2caom2_cmd_in_out()

        self.logger.debug('read the xml into memory from the file')
        self.observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(self.observation)

        self.logger.debug('write the observation to disk for debugging')
        self._write_model(self.observation)

        self.logger.debug('the observation exists, delete it')
        self._repo_cmd_delete_client(self.observation)

        self.logger.debug('store the xml')
        self._repo_cmd_create_client(self.observation)

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute')


class MetaUpdateObservation(CaomExecute):
    """
    Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will handle the 1 observation ID to n file names
    relationship when executing fits2caom2, based only on existing
    metadata from a CAOM2 record.
    """

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client,
                 observation, meta_visitors, observable):
        super(MetaUpdateObservation, self).__init__(
            config, mc.TaskType.INGEST_OBS, storage_name, command_name,
            cred_param, cadc_client, caom_repo_client, meta_visitors,
            observable)
        self.observation = observation
        # set the pre-conditions, as none of this is known, until
        # the observation is figured out
        self.fname = None
        self.lineage = None
        self.external_urls_param = ''
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug(f'Begin execute.')
        self.logger.debug('the steps:')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('set up parameters for to_caom2 call')
        self._set_parameters()

        self.logger.debug('write the observation to disk for next step')
        self._write_model(self.observation)

        self.logger.debug('update an existing observation')
        self._fits2caom2_cmd_in_out()

        self.logger.debug('read the xml into memory from the file')
        self.observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(self.observation)

        self.logger.debug('write the observation to disk for debugging')
        self._write_model(self.observation)

        self.logger.debug('update the observation')
        self._repo_cmd_update_client(self.observation)

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug(f'End execute')

    def _set_parameters(self):
        lineage = ''
        # make a dict of product ids and artifact uris
        for plane in self.observation.planes.values():
            for artifact in plane.artifacts.values():
                if 'fits' in artifact.uri:
                    scheme, archive, file_name = mc.decompose_uri(artifact.uri)
                    result = mc.get_lineage(
                        archive, plane.product_id, file_name, scheme)
                    lineage = f'{lineage} {result}'

        self.lineage = lineage


class LocalMetaCreate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo create."""

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client, meta_visitors,
                 observable):
        super(LocalMetaCreate, self).__init__(
            config, mc.TaskType.INGEST, storage_name, command_name, cred_param,
            cadc_client, caom_repo_client, meta_visitors, observable)
        self._define_local_dirs(storage_name)
        self.fname = storage_name.fname_on_disk
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('the observation does not exist, so go '
                          'straight to generating the xml, as the main_app '
                          'will retrieve the headers')
        self._fits2caom2_cmd_local()

        self.logger.debug('read the xml from disk')
        observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(observation)

        self.logger.debug('write the observation to disk for debugging')
        self._write_model(observation)

        self.logger.debug('store the xml')
        self._repo_cmd_create_client(observation)

        self.logger.debug('End execute')


class LocalMetaDeleteCreate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo delete followed by
    a create, because an update will not support a Simple->Composite
    or Composite->Simple type change for the Observation
    structure."""

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client, observation,
                 meta_visitors, observable):
        super(LocalMetaDeleteCreate, self).__init__(
            config, mc.TaskType.INGEST, storage_name, command_name, cred_param,
            cadc_client, caom_repo_client, meta_visitors, observable)
        self._define_local_dirs(storage_name)
        self.observation = observation
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('write the observation to disk for next step')
        self._write_model(self.observation)

        self.logger.debug('make a new observation from an existing '
                          'observation')
        self._fits2caom2_cmd_in_out_local()

        self.logger.debug('read the xml from disk')
        observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(observation)

        self.logger.debug('write the observation to disk for debugging')
        self._write_model(self.observation)

        self.logger.debug('the observation exists, delete it')
        self._repo_cmd_delete_client(self.observation)

        self.logger.debug('store the xml')
        self._repo_cmd_create_client(observation)

        self.logger.debug('End execute')


class LocalMetaUpdate(CaomExecute):
    """Defines the pipeline step for Collection ingestion of metadata into CAOM.
    This requires access to only header information.

    This pipeline step will execute a caom2-repo update."""

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client, observation,
                 meta_visitors, observable):
        super(LocalMetaUpdate, self).__init__(
            config, mc.TaskType.INGEST, storage_name, command_name, cred_param,
            cadc_client, caom_repo_client, meta_visitors, observable)
        self._define_local_dirs(storage_name)
        self.observation = observation
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('write the observation to disk for next step')
        self._write_model(self.observation)

        self.logger.debug('generate the xml, as the main_app will retrieve '
                          'the headers')
        self._fits2caom2_cmd_in_out_local()

        self.logger.debug('read the xml from disk')
        self.observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(self.observation)

        self.logger.debug('write the updated xml to disk for debugging')
        self._write_model(self.observation)

        self.logger.debug('store the xml')
        self._repo_cmd_update_client(self.observation)

        self.logger.debug('End execute')


class MetaVisit(CaomExecute):
    """Defines the pipeline step for Collection augmentation by a visitor
     of metadata into CAOM. This assumes a record already exists in CAOM,
     and the update DOES NOT require access to either the header or the data.

    This pipeline step will execute a caom2-repo update.

    This functionality exists as Séverin's request.
    """

    def __init__(self, config, storage_name, cred_param,
                 cadc_client, caom_repo_client, meta_visitors,
                 observable):
        super(MetaVisit, self).__init__(
            config, mc.TaskType.VISIT, storage_name, command_name=None,
            cred_param=cred_param, cadc_client=cadc_client,
            caom_repo_client=caom_repo_client,
            meta_visitors=meta_visitors, observable=observable)
        self.fname = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug('Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('retrieve the existing observation, if it exists')
        observation = self._repo_cmd_read_client()

        self.logger.debug('the metadata visitors')
        self._visit_meta(observation)

        self.logger.debug('write the updated xml to disk for debugging')
        self._write_model(observation)

        self.logger.debug('store the xml')
        self._repo_cmd_update_client(observation)

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug('End execute')


class DataVisit(CaomExecute):
    """Defines the pipeline step for all the operations that
    require access to the file on disk. The data must be retrieved
    from a separate source.

    :param transferrer: instance of transfer_composable.Transfer -
        how to get data from any DataSource for execution.
    """

    def __init__(self, config, storage_name, cred_param,
                 cadc_client,
                 caom_repo_client, data_visitors, task_type,
                 observable, transferrer):
        super(DataVisit, self).__init__(
            config, task_type=task_type, storage_name=storage_name,
            command_name=None, cred_param=cred_param, cadc_client=cadc_client,
            caom_repo_client=caom_repo_client, meta_visitors=None,
            observable=observable)
        self._data_visitors = data_visitors
        self._log_file_directory = config.log_file_directory
        self._transferrer = transferrer
        self._logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self._logger.debug('Begin execute')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug('get the input file')
        self._transferrer.get(self._storage_name.source_name, self._fqn)

        self.logger.debug('get the observation for the existing model')
        observation = self._repo_cmd_read_client()

        self.logger.debug('execute the data visitors')
        self._visit_data(observation)

        self.logger.debug('write the observation to disk for debugging')
        self._write_model(observation)

        self.logger.debug('store the updated xml')
        self._repo_cmd_update_client(observation)

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self._logger.debug('End execute.')

    def _visit_data(self, observation):
        """Execute the visitors that require access to the full data content
        of a file."""
        kwargs = {'working_directory': self.working_dir,
                  'science_file': self.fname,
                  'log_file_directory': self._log_file_directory,
                  'cadc_client': self.cadc_client,
                  'caom_repo_client': self.caom_repo_client,
                  'stream': self.stream,
                  'observable': self.observable}
        for visitor in self._data_visitors:
            try:
                self.logger.debug(f'Visit for {visitor}')
                visitor.visit(observation, **kwargs)
            except Exception as e:
                raise mc.CadcException(e)


class LocalDataVisit(DataVisit):
    """Defines the pipeline step for all the operations that
    require access to the file on disk. This class assumes it has access to
    the files on disk - i.e. there is not need to retrieve the files from
    the CADC storage system, but there is a need to update CAOM
    entries with the service.
    """

    def __init__(self, config, storage_name, cred_param,
                 cadc_client, caom_repo_client, data_visitors,
                 observable):
        super(LocalDataVisit, self).__init__(
            config, storage_name=storage_name, cred_param=cred_param,
            cadc_client=cadc_client,
            caom_repo_client=caom_repo_client, data_visitors=data_visitors,
            task_type=mc.TaskType.MODIFY, observable=observable,
            transferrer=tc.Transfer())
        self._define_local_dirs(storage_name)
        self.fname = storage_name.fname_on_disk
        self.prev_fname = storage_name.prev
        self.thumb_fname = storage_name.thumb
        self._logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self._logger.debug(f'Begin execute')

        self._logger.debug('get the observation for the existing model')
        observation = self._repo_cmd_read_client()

        self._logger.debug('execute the data visitors')
        self._visit_data(observation)

        self._logger.debug('write the updated xml to disk for debugging')
        self._write_model(observation)

        self._logger.debug('store the updated xml')
        self._repo_cmd_update_client(observation)

        self._logger.debug(f'End execute')


class DataScrape(DataVisit):
    """Defines the pipeline step for Collection generation and ingestion of
    operations that require access to the file on disk, with no update to the
    service at the end. This class assumes it has access to the files on disk.
    The organization of this class assumes the 'Scrape' task has been done
    previously, so the model instance exists on disk."""

    def __init__(self, config, storage_name, data_visitors, observable):
        super(DataScrape, self).__init__(
            config, storage_name, cred_param='',
            cadc_client=None, caom_repo_client=None,
            data_visitors=data_visitors, task_type=mc.TaskType.SCRAPE,
            observable=observable, transferrer=tc.Transfer())
        self._define_local_dirs(storage_name)
        self.fname = storage_name.fname_on_disk
        self.log_file_directory = config.log_file_directory
        self.prev_fname = storage_name.prev
        self.thumb_fname = storage_name.thumb
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug(f'Begin execute')

        self.logger.debug('get observation for the existing model from disk')
        observation = self._read_model()

        self.logger.debug('execute the data visitors')
        self._visit_data(observation)

        self.logger.debug('output the updated xml')
        self._write_model(observation)

        self.logger.debug(f'End execute')


class Store(CaomExecute):
    """Defines the pipeline step for Collection storage of a file. This
    requires access to the file on disk."""

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client, observable,
                 transferrer):
        super(Store, self).__init__(
            config, mc.TaskType.STORE, storage_name, command_name, cred_param,
            cadc_client, caom_repo_client, meta_visitors=None,
            observable=observable)
        self.stream = config.stream
        self.multiple_files = storage_name.multiple_files(self.working_dir)
        # handle the case of a URI source
        self._destination_f_names = \
            storage_name.multiple_files(self.working_dir)
        if len(self.multiple_files) == 0:
            self.multiple_files = [storage_name.entry]
            self._destination_f_names = [storage_name.file_name]
            self._destination_uri = [storage_name.file_uri]
        self._transferrer = transferrer
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug(f'Begin execute')

        self.logger.debug('create the work space, if it does not exist')
        self._create_dir()

        self.logger.debug(f'Store {len(self.multiple_files)} files to ad.')
        for index, entry in enumerate(self.multiple_files):
            self._fqn = f'{self.working_dir}/{self._destination_f_names[index]}'
            self.logger.debug(f'Retrieve {entry}')
            self._transferrer.get(entry, self._fqn)

            self.fname = self._destination_f_names[index]
            self.logger.debug(f'store the input file {self.fname}')
            self._cadc_put(self._destination_uri[index])

        self.logger.debug('clean up the workspace')
        self._cleanup()

        self.logger.debug(f'End execute')


class LocalStore(Store):
    """Defines the pipeline step for Collection storage of a file. This
    requires access to the file on disk. The file originates from local
    disk."""

    def __init__(self, config, storage_name, command_name, cred_param,
                 cadc_client, caom_repo_client, observable,
                 transferrer):
        super(LocalStore, self).__init__(
            config, storage_name, command_name, cred_param,
            cadc_client, caom_repo_client, observable, transferrer)
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug(f'Begin execute')

        self.logger.debug(f'Store {len(self.multiple_files)} files to ad.')
        for index, entry in enumerate(self.multiple_files):
            self.fname = self._destination_f_names[index]
            self.logger.debug(f'store the input file {self.fname}')
            self._cadc_put(self._destination_uri[index])

        self.logger.debug(f'End execute')


class Scrape(CaomExecute):
    """Defines the pipeline step for Collection creation of a CAOM model
    observation. The file containing the metadata is located on disk.
    No record is written to a web service."""

    def __init__(self, config, storage_name, command_name, observable,
                 meta_visitors):
        super(Scrape, self).__init__(
            config, mc.TaskType.SCRAPE, storage_name, command_name,
            cred_param='', cadc_client=None, caom_repo_client=None,
            meta_visitors=meta_visitors, observable=observable)
        self._define_local_dirs(storage_name)
        self.fname = storage_name.fname_on_disk
        if self.fname is None:
            self.fname = storage_name.file_name
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug(f'Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('generate the xml from the file on disk')
        self._fits2caom2_cmd_local(connected=False)

        self.logger.debug('get observation for the existing model from disk')
        observation = self._read_model()

        self.logger.error('the metadata visitors')
        self._visit_meta(observation)

        self.logger.debug('write the updated xml to disk for debugging')
        self._write_model(observation)

        self.logger.debug(f'End execute')


class ScrapeUpdate(CaomExecute):
    """Defines the pipeline step for Collection creation of a CAOM model
    observation. The file containing the metadata is located on disk.
    No record is written to a web service."""

    def __init__(self, config, storage_name, command_name, observable,
                 meta_visitors):
        super(ScrapeUpdate, self).__init__(
            config, mc.TaskType.SCRAPE, storage_name, command_name,
            cred_param='', cadc_client=None, caom_repo_client=None,
            meta_visitors=meta_visitors, observable=observable)
        self._define_local_dirs(storage_name)
        self.fname = storage_name.fname_on_disk
        if self.fname is None:
            self.fname = storage_name.file_name
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        self.logger.debug(f'Begin execute')
        self.logger.debug('the steps:')

        self.logger.debug('generate the xml from the file on disk')
        self._fits2caom2_cmd_in_out_local(connected=False)

        self.logger.debug('get observation for the existing model from disk')
        observation = self._read_model()

        self.logger.debug('the metadata visitors')
        self._visit_meta(observation)

        self.logger.debug('write the updated xml to disk for debugging')
        self._write_model(observation)

        self.logger.debug(f'End execute')


class OrganizeChooser(object):
    """Extend this class to provide a way to make collection-specific
    complex conditions available within the OrganizeExecute class."""
    def __init__(self):
        pass

    def needs_delete(self, observation):
        return False

    def use_compressed(self, f=None):
        return False


class OrganizeExecutes(object):
    """How to turn on/off various task types in a CaomExecute pipeline."""

    def __init__(
        self,
        config,
        command_name,
        meta_visitors,
        data_visitors,
        chooser=None,
        store_transfer=None,
        modify_transfer=None,
        cadc_client=None,
        caom_client=None
    ):
        """
        Why there is support for two transfer instances:
        - the store_transfer instance may do an http, ftp, or vo transfer
            from an external source, for the purposes of storage
        - the modify_transfer instance probably does a CADC retrieval,
            so that metadata production that relies on the data content
            (e.g. preview generation) can occur

        :param config:
        :param command_name extension of fits2caom2 for the collection
        :param meta_visitors List of metadata visit methods.
        :param data_visitors List of data visit methods.
        :param chooser:
        :param store_transfer Transfer implementation for retrieving files
        :param modify_transfer Transfer implementation for retrieving files
        :param cadc_client CadcDataClient/vos.Client, depending on the
            Features.supports_latest_client flag value
        :param caom_client CAOM2RepoClient
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
        self.observable = mc.Observable(mc.Rejected(self.rejected_fqn),
                                        mc.Metrics(config))
        self._command_name = command_name
        self._meta_visitors = meta_visitors
        self._data_visitors = data_visitors
        self._modify_transfer = modify_transfer
        self._store_transfer = store_transfer
        # use the same Observable everywhere
        if modify_transfer is not None:
            self._modify_transfer.observable = self.observable
        if store_transfer is not None:
            self._store_transfer.observable = self.observable
        self._cadc_client = cadc_client
        self._caom_client = caom_client
        self._cred_param = self._define_cred_param()
        self._log_h = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(config.logging_level)

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

        If log_to_file is set to False, don't track the failure log file or
        the retry log file entries, because the application should leave no
        logging trace.

        :obs_id observation ID being processed
        :file_name file name being processed
        :e Exception to log - the entire stack trace, which, if logging
            level is not set to debug, will be lost for debugging purposes.
        """
        self._count_timeouts(stack_trace)
        if self.config.log_to_file:
            with open(self.failure_fqn, 'a') as failure:
                if e.args is not None and len(e.args) > 1:
                    min_error = e.args[0]
                else:
                    min_error = str(e)
                failure.write(f'{datetime.now()} {storage_name.obs_id} '
                              f'{storage_name.file_name} {min_error}\n')

        # only retry entries that are not permanently marked as rejected
        reason = mc.Rejected.known_failure(stack_trace)
        if reason == mc.Rejected.NO_REASON:
            if self.config.log_to_file:
                with open(self.retry_fqn, 'a') as retry:
                    if (hasattr(storage_name, '_entry') and
                            storage_name.entry is not None):
                        retry.write(f'{storage_name.entry}\n')
                    else:
                        if self.config.use_local_files:
                            retry.write(f'{storage_name.fname_on_disk}\n')
                        elif self.config.features.use_file_names:
                            retry.write(f'{storage_name.file_name}\n')
                        else:
                            retry.write(f'{storage_name.obs_id}\n')
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
        # if log_to_file is set to False, leave no logging trace
        if self.config.log_to_file:
            success = open(self.success_fqn, 'a')
            try:
                success.write(f'{datetime.now()} {obs_id} {file_name} '
                              f'{execution_s:.2f}\n')
            finally:
                success.close()
        logging.debug('******************************************************')
        logging.info(f'Progress - record {self.success_count} of '
                     f'{self.complete_record_count} records processed in '
                     f'{execution_s:.2f} s.')
        logging.debug('******************************************************')

    def set_log_location(self):
        self.set_log_files(self.config)
        if self.config.log_to_file:
            mc.create_dir(self.config.log_file_directory)
            now_s = datetime.utcnow().timestamp()
            for fqn in [self.success_fqn, self.failure_fqn, self.retry_fqn]:
                OrganizeExecutes.init_log_file(fqn, now_s)
        self._success_count = 0

    def is_rejected(self, storage_name):
        """Common code to use the appropriate identifier when checking for
        rejected entries."""
        if self.config.features.use_urls:
            result = self.observable.rejected.is_bad_metadata(
                storage_name.url)
        elif self.config.features.use_file_names:
            result = self.observable.rejected.is_bad_metadata(
                storage_name.file_name)
        else:
            result = self.observable.rejected.is_bad_metadata(
                storage_name.obs_id)
        if result:
            logging.info(f'Rejected observation {storage_name.obs_id} because '
                         f'of bad metadata')
        return result

    def _define_cred_param(self):
        """Common code to figure out which credentials to use when
        creating instances clients for CADC services."""
        if (self.config.proxy_fqn is not None and os.path.exists(
                self.config.proxy_fqn)):
            logging.debug(f'Using proxy certificate {self.config.proxy_fqn} '
                          f'for credentials.')
            cred_param = f'--cert {self.config.proxy_fqn}'
        elif (self.config.netrc_file is not None and os.path.exists(
                self.config.netrc_file)):
            logging.debug(f'Using netrc file {self.config.netrc_file} for '
                          f'credentials.')
            cred_param = f'--netrc {self.config.netrc_file}'
        else:
            cred_param = ''
            logging.warning(
                'No credentials provided (proxy certificate or netrc file).')
            logging.warning(f'Proxy certificate is {self.config.proxy_fqn}, '
                            f'netrc file is {self.config.netrc_file}.')
        return cred_param

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
        if (e is not None and ('Read timed out' in e or
                               'reset by peer' in e or
                               'ConnectTimeoutError' in e or
                               'Broken pipe' in e)):
            self._timeout += 1

    @property
    def command_name(self):
        return self._command_name

    def _set_up_file_logging(self, storage_name):
        """Configure logging to a separate file for each entry being
        processed."""
        if self.config.log_to_file:
            log_fqn = os.path.join(self.config.working_directory,
                                   storage_name.log_file)
            if self.config.log_file_directory is not None:
                log_fqn = os.path.join(self.config.log_file_directory,
                                       storage_name.log_file)
            self._log_h = logging.FileHandler(log_fqn)
            formatter = logging.Formatter(
                '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s')
            self._log_h.setLevel(self.config.logging_level)
            self._log_h.setFormatter(formatter)
            logging.getLogger().addHandler(self._log_h)

    def _unset_file_logging(self):
        """Turn off the logging to the separate file for each entry being
        processed."""
        if self.config.log_to_file:
            logging.getLogger().removeHandler(self._log_h)

    def choose(self, storage_name):
        """The logic that decides which descendants of CaomExecute to
        instantiate. This is based on the content of the config.yml file
        for an application.
        :storage_name StorageName extension that handles the naming rules for
            a file.
        """
        executors = []
        if mc.TaskType.SCRAPE not in self.task_types:
            for entry in [self._modify_transfer, self._store_transfer]:
                if entry is not None:
                    # set only for Transfer specializations that have a
                    # cadc_client attribute (HttpTransfer, FtpTransfer do not)
                    if hasattr(entry, '_cadc_client'):
                        entry.cadc_client = self._cadc_client
        for task_type in self.task_types:
            self._logger.debug(task_type)
            if task_type == mc.TaskType.SCRAPE:
                model_fqn = os.path.join(self.config.working_directory,
                                         storage_name.model_file_name)
                exists = os.path.exists(model_fqn)
                if exists:
                    if self.config.use_local_files:
                        executors.append(
                            ScrapeUpdate(
                                self.config, storage_name, self._command_name,
                                self.observable, self._meta_visitors))
                    else:
                        raise mc.CadcException(
                            'use_local_files must be True with '
                            'Task Type "SCRAPE"')
                else:
                    if self.config.use_local_files:
                        executors.append(Scrape(
                            self.config, storage_name, self._command_name,
                            self.observable, self._meta_visitors))
                    else:
                        raise mc.CadcException(
                            'use_local_files must be True with '
                            'Task Type "SCRAPE"')
            elif task_type == mc.TaskType.STORE:
                if self.config.use_local_files:
                    executors.append(
                        LocalStore(
                            self.config, storage_name, self._command_name,
                            self._cred_param, self._cadc_client,
                            self._caom_client, self.observable,
                            self._store_transfer))
                else:
                    executors.append(
                        Store(
                            self.config, storage_name, self._command_name,
                            self._cred_param, self._cadc_client,
                            self._caom_client, self.observable,
                            self._store_transfer))
            elif task_type == mc.TaskType.INGEST:
                observation = CaomExecute.repo_cmd_get_client(
                    self._caom_client, self.config.collection,
                    storage_name.obs_id, self.observable.metrics)
                if observation is None:
                    if self.config.use_local_files:
                        executors.append(
                            LocalMetaCreate(
                                self.config, storage_name, self._command_name,
                                self._cred_param, self._cadc_client,
                                self._caom_client, self._meta_visitors,
                                self.observable))
                    else:
                        executors.append(MetaCreate(
                            self.config, storage_name, self._command_name,
                            self._cred_param, self._cadc_client,
                            self._caom_client,
                            self._meta_visitors, self.observable))
                else:
                    if self.config.use_local_files:
                        if (self.chooser is not None and
                                self.chooser.needs_delete(observation)):
                            executors.append(
                                LocalMetaDeleteCreate(
                                    self.config, storage_name,
                                    self._command_name,
                                    self._cred_param, self._cadc_client,
                                    self._caom_client, observation,
                                    self._meta_visitors, self.observable))
                        else:
                            executors.append(
                                LocalMetaUpdate(
                                    self.config, storage_name,
                                    self._command_name,
                                    self._cred_param, self._cadc_client,
                                    self._caom_client, observation,
                                    self._meta_visitors, self.observable))
                    else:
                        if (self.chooser is not None and
                                self.chooser.needs_delete(observation)):
                            executors.append(
                                MetaDeleteCreate(
                                    self.config, storage_name,
                                    self._command_name,
                                    self._cred_param, self._cadc_client,
                                    self._caom_client, observation,
                                    self._meta_visitors, self.observable))
                        else:
                            executors.append(
                                MetaUpdate(
                                    self.config, storage_name,
                                    self._command_name, self._cred_param,
                                    self._cadc_client, self._caom_client,
                                    observation, self._meta_visitors,
                                    self.observable))
            elif task_type == mc.TaskType.INGEST_OBS:
                observation = CaomExecute.repo_cmd_get_client(
                    self._caom_client, self.config.collection,
                    storage_name.obs_id, self.observable.metrics)
                if observation is None:
                    raise mc.CadcException(
                        f'"INGEST_OBS" is an update-only task type for '
                        f'{storage_name.obs_id}.')
                else:
                    if self.config.use_local_files:
                        raise NotImplementedError
                    else:
                        executors.append(
                            MetaUpdateObservation(
                                self.config, storage_name, self._command_name,
                                self._cred_param, self._cadc_client,
                                self._caom_client,
                                observation, self._meta_visitors,
                                self.observable))
            elif task_type == mc.TaskType.MODIFY:
                if storage_name.is_feasible:
                    if self.config.use_local_files:
                        if (executors is not None and len(executors) > 0 and
                                (isinstance(executors[0], Scrape) or
                                 isinstance(executors[0], ScrapeUpdate))):
                            executors.append(
                                DataScrape(self.config, storage_name,
                                           self._data_visitors,
                                           self.observable))
                        else:
                            executors.append(
                                LocalDataVisit(
                                    self.config, storage_name,
                                    self._cred_param,
                                    self._cadc_client, self._caom_client,
                                    self._data_visitors, self.observable))
                    else:
                        executors.append(DataVisit(
                            self.config, storage_name, self._cred_param,
                            self._cadc_client, self._caom_client,
                            self._data_visitors, mc.TaskType.MODIFY,
                            self.observable, self._modify_transfer))
                else:
                    self._logger.info(f'Skipping the MODIFY task for '
                                      f'{storage_name.file_name}.')
            elif task_type == mc.TaskType.VISIT:
                executors.append(MetaVisit(
                    self.config, storage_name, self._cred_param,
                    self._cadc_client, self._caom_client, self._meta_visitors,
                    self.observable))
            elif task_type == mc.TaskType.DEFAULT:
                pass
            else:
                raise mc.CadcException(
                    f'Do not understand task type {task_type}')
        return executors

    def do_one(self, storage_name):
        """Process one entry.
        :param storage_name instance of StorageName for the collection
        """
        self._logger.debug(f'Begin do_one {storage_name}')
        self._set_up_file_logging(storage_name)
        start_s = datetime.utcnow().timestamp()
        try:
            if self.is_rejected(storage_name):
                self.capture_failure(storage_name, 
                                     BaseException('StorageName.is_rejected'),
                                     'Rejected')
                # successful rejection of the execution case
                return 0
            executors = self.choose(storage_name)
            for executor in executors:
                self._logger.info(f'Step {executor.task_type} with '
                                  f'{executor.__class__.__name__} for '
                                  f'{storage_name.obs_id}')
                executor.execute(context=None)
            if len(executors) > 0:
                self.capture_success(storage_name.obs_id,
                                     storage_name.file_name, start_s)
                return 0
            else:
                self._logger.info(f'No executors for {storage_name}')
                return -1  # cover the case where file name validation fails
        except Exception as e:
            self.capture_failure(storage_name, e, traceback.format_exc())
            self._logger.warning(f'Execution failed for {storage_name.obs_id} '
                                 f'with {e}')
            self._logger.debug(traceback.format_exc())
            return -1
        finally:
            self._unset_file_logging()

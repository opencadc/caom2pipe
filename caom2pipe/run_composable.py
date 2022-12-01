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

"""
On the structure and responsibility of the 'run' methods:
- config file reads are in the 'composable' modules of the individual
  pipelines, so that collection-specific changes will not be surprises
  during execution
- exception handling is also in execute_composable.CaomExecute
  specializations, except for retry loops

"""

import logging
import os
import traceback

from collections import deque
from datetime import datetime, timezone
from time import sleep

from caom2pipe import client_composable as cc
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc
from caom2pipe import data_source_composable
from caom2pipe import name_builder_composable
from caom2pipe import reader_composable
from caom2pipe import transfer_composable

__all__ = [
    'common_runner_init',
    'get_utc_now',
    'get_utc_now_tz',
    'run_by_state',
    'run_by_todo',
    'StateRunner',
    'TodoRunner',
]


class TodoRunner:
    """
    This class brings together the mechanisms for identifying the
    lists of work to be done (DataSource extensions), and the mechanisms for
    translating a list of work into a collection-specific name
    (StorageNameBuilder extensions).
    """

    def __init__(
        self,
        config,
        organizer,
        builder,
        data_source,
        metadata_reader,
        observable,
        reporter,
    ):
        self._builder = builder
        self._data_source = data_source
        self._metadata_reader = metadata_reader
        self._config = config
        self._organizer = organizer
        # the list of work to be done, containing whatever is returned from
        # the DataSource instance
        self._todo_list = []
        self._observable = observable
        self._reporter = reporter
        self._logger = logging.getLogger(self.__class__.__name__)

    def _build_todo_list(self):
        self._logger.debug(f'Begin _build_todo_list.')
        self._todo_list = self._data_source.get_work()
        self._logger.info(f'Processing {self._reporter.all} records.')
        self._logger.debug('End _build_todo_list.')

    def _finish_run(self):
        mc.create_dir(self._config.log_file_directory)
        self._observable.rejected.persist_state()
        self._observable.metrics.capture()
        msg = f'Done, processed {self._reporter.success} of {self._reporter.all} correctly.'
        self._logger.info('-' * len(msg))
        self._logger.info(msg)
        self._logger.info('-' * len(msg))

    def _process_entry(self, entry, current_count):
        self._logger.debug(f'Begin _process_entry for {entry}.')
        storage_name = None
        try:
            storage_name = self._builder.build(entry)
            if storage_name.is_valid():
                result = self._organizer.do_one(storage_name)
            else:
                self._logger.error(
                    f'{storage_name.obs_id} failed naming validation check.'
                )
                self._reporter.capture_failure(
                    storage_name, BaseException('Invalid name format'), 'Invalid name format.'
                )
                result = -1
        except Exception as e:
            if storage_name is None:
                # keep going through storage name build failures
                self._logger.debug(traceback.format_exc())
                self._logger.warning(
                    f'StorageName construction failed. Using a default '
                    f'instance for {entry}, for logging only.'
                )
                storage_name = mc.StorageName(
                    obs_id=entry, source_names=[entry]
                )
            self._reporter.capture_failure(storage_name, e, traceback.format_exc())
            self._logger.info(
                f'Execution failed for {storage_name.file_name} with {e}'
            )
            self._logger.debug(traceback.format_exc())
            # keep processing the rest of the entries, so don't throw
            # this or any other exception at this point
            result = -1
        try:
            self._data_source.clean_up(entry, result, current_count)
        except Exception as e:
            self._logger.info(f'Cleanup failed for {entry} with {e}')
            self._logger.debug(traceback.format_exc())
            result = -1
        self._logger.debug(f'End _process_entry.')
        return result

    def _run_todo_list(self, current_count):
        """
        :param current_count: int - current retry count - needs to be passed
            to _process_entry.
        """
        self._logger.debug('Begin _run_todo_list.')
        result = 0
        while len(self._todo_list) > 0:
            entry = self._todo_list.popleft()
            result |= self._process_entry(entry, current_count)
            self._metadata_reader.reset()
        self._finish_run()
        self._logger.debug('End _run_todo_list.')
        return result

    def _reset_for_retry(self, count):
        self._config.update_for_retry(count)
        # the log location changes for each retry
        self._reporter.set_log_location(self._config)
        # self._reporter.reset_for_retry()
        # change the data source handling for the retry, but preserve the original
        # clean_up behaviour
        original_data_source_cleanup = self._data_source.clean_up
        self._data_source = data_source_composable.TodoFileDataSource(self._config)
        self._data_source.reporter = self._reporter
        self._data_source.clean_up = original_data_source_cleanup

    def report(self):
        self._reporter.report()

    def run(self):
        self._logger.debug('Begin run.')
        self._build_todo_list()
        # have the choose call here, so that retries don't change the set of tasks to be executed
        self._organizer.choose()
        result = self._run_todo_list(current_count=0)
        self._logger.debug('End run.')
        return result

    def run_retry(self):
        self._logger.debug('Begin retry run.')
        result = 0
        if self._config.need_to_retry():
            for count in range(0, self._config.retry_count):
                self._logger.warning(
                    f'Beginning retry {count + 1} in {os.getcwd()}'
                )
                self._reset_for_retry(count)
                # make another file list
                self._build_todo_list()
                self._reporter.capture_retry()
                decay_interval = self._config.retry_decay * (count + 1) * 60
                self._logger.warning(f'Retry {self._reporter.all} entries at {decay_interval} seconds from now.')
                sleep(decay_interval)
                result |= self._run_todo_list(current_count=count + 1)
                if not self._config.need_to_retry():
                    break
            self._logger.warning(f'Done retry attempts with result {result}.')
        else:
            self._logger.info('No failures to be retried.')
        self._logger.debug('End retry run.')
        return result


class StateRunner(TodoRunner):
    """
    This class brings together the mechanisms for identifying the time-boxed lists of work to be done
    (DataSource specializations), and the mechanisms for translating a list of work into a collection-specific name
    (StorageNameBuilder specializations).
    """

    def __init__(
        self,
        config,
        organizer,
        builder,
        data_source,
        metadata_reader,
        bookmark_name,
        observable,
        reporter,
        max_ts=None,
    ):
        super().__init__(
            config, organizer, builder, data_source, metadata_reader, observable, reporter
        )
        self._bookmark_name = bookmark_name
        max_ts_in_s = None
        if max_ts is not None:
            max_ts_in_s = mc.convert_to_ts(max_ts)
        # end time is a datetime.timestamp
        self._end_time = (
            get_utc_now_tz().timestamp()
            if max_ts_in_s is None
            else max_ts_in_s
        )

    def _record_progress(
        self, count, cumulative_count, start_time, save_time
    ):
        start_time_dt = datetime.utcfromtimestamp(start_time)
        save_time_dt = datetime.utcfromtimestamp(save_time)
        with open(self._config.progress_fqn, 'a') as progress:
            progress.write(
                f'{datetime.now()} current:: {save_time_dt} {count} '
                f'since:: {start_time_dt}:: {cumulative_count}\n'
            )

    def _wrap_state_save(self, state, save_time):
        state.save_state(
            self._bookmark_name, datetime.utcfromtimestamp(save_time)
        )

    def run(self):
        """
        Uses an iterable with an instance of StateRunnerMeta.

        :return: 0 for success, -1 for failure
        """
        self._logger.debug(f'Begin run state for {self._bookmark_name}')
        if not os.path.exists(os.path.dirname(self._config.progress_fqn)):
            os.makedirs(os.path.dirname(self._config.progress_fqn))

        state = mc.State(self._config.state_fqn)
        if self._data_source.start_time_ts is None:
            temp = state.get_bookmark(self._bookmark_name)
            start_time = mc.convert_to_ts(temp)
        else:
            start_time = self._data_source.start_time_ts

        # make sure prev_exec_time is offset-aware type datetime.timestamp
        prev_exec_time = start_time
        incremented_ts = mc.increment_time_tz(
            prev_exec_time, self._config.interval
        ).timestamp()
        exec_time = min(incremented_ts, self._end_time)

        self._logger.debug(
            f'Starting at {datetime.utcfromtimestamp(start_time)}, ending at '
            f'{datetime.utcfromtimestamp(self._end_time)}'
        )
        result = 0
        if prev_exec_time == self._end_time:
            self._logger.info(
                f'Start time is the same as end time '
                f'{datetime.utcfromtimestamp(start_time)}, stopping.'
            )
            exec_time = prev_exec_time
        else:
            cumulative = 0
            result = 0
            self._organizer.choose()
            while exec_time <= self._end_time:
                self._logger.info(
                    f'Processing from '
                    f'{datetime.utcfromtimestamp(prev_exec_time)} to '
                    f'{datetime.utcfromtimestamp(exec_time)}'
                )
                save_time = exec_time
                self._organizer.success_count = 0
                self._reporter.set_log_location(self._config)
                entries = self._data_source.get_time_box_work(prev_exec_time, exec_time)
                num_entries = len(entries)

                if num_entries > 0:
                    self._logger.info(f'Processing {self._reporter.all} entries.')
                    pop_action = entries.pop
                    if isinstance(entries, deque):
                        pop_action = entries.popleft
                    while len(entries) > 0:
                        entry = pop_action()
                        result |= self._process_entry(entry.entry_name, 0)
                        save_time = min(
                            mc.convert_to_ts(entry.entry_ts), exec_time
                        )
                    # this reset call is outside the while process_entry loop
                    # for GEMINI which gets all the metadata for an interval in
                    # a single call, and it wouldn't be polite to throw away
                    # all the metadata that will be just need to be retrieved
                    # again for each record
                    self._metadata_reader.reset()
                    self._finish_run()

                self._record_progress(
                    num_entries, cumulative, start_time, save_time
                )
                state.save_state(
                    self._bookmark_name, datetime.utcfromtimestamp(save_time)
                )

                if exec_time == self._end_time:
                    # the last interval will always have the exec time
                    # equal to the end time, which will fail the while check
                    # so leave after the last interval has been processed
                    #
                    # but the while <= check is required so that an interval
                    # smaller than exec_time -> end_time will get executed,
                    # so don't get rid of the '=' in the while loop
                    # comparison, just because this one exists
                    break
                prev_exec_time = exec_time
                new_time = mc.increment_time_tz(
                    prev_exec_time, self._config.interval
                ).timestamp()
                exec_time = min(new_time, self._end_time)

        state.save_state(
            self._bookmark_name, datetime.utcfromtimestamp(exec_time)
        )
        msg = (
            f'Done for {self._bookmark_name}, saved state is '
            f'{datetime.utcfromtimestamp(exec_time)}'
        )
        self._logger.info('=' * len(msg))
        self._logger.info(msg)
        self._logger.info(f'{self._reporter.success} of {self._reporter.all} records processed correctly.')
        self._logger.info('=' * len(msg))
        return result


def _set_logging(config):
    formatter = logging.Formatter(
        '%(asctime)s:%(levelname)-7s:%(name)-12s:%(lineno)-4d:%(message)s'
    )
    for handler in logging.getLogger().handlers:
        handler.setLevel(config.logging_level)
        handler.setFormatter(formatter)
    logging.getLogger('root').setLevel(config.logging_level)


def get_utc_now():
    """So that utcnow can be mocked."""
    return datetime.utcnow()


def get_utc_now_tz():
    """So that utcnow can be mocked. And serendipitously, the guidance from
    the dateutil maintainer is not to use this anymore:
    https://blog.ganssle.io/articles/2019/11/utcnow.html
    :return an offset-aware datetime.datetime
    """
    return datetime.now(tz=timezone.utc)


def common_runner_init(
    config,
    clients,
    name_builder,
    source,
    modify_transfer,
    metadata_reader,
    state,
    store_transfer,
    meta_visitors,
    data_visitors,
    chooser,
    application,
):
    """The common initialization code between TodoRunner and StateRunner uses. <collection>2caom2 implementations can
    use the defaults created here for the 'run' call, or they can provide their own specializations of the various
    classes required to store data and ingest metadata at CADC.

    :param config Config instance
    :param clients: ClientCollection instance
    :param name_builder NameBuilder extension that creates an instance of a StorageName extension, from an entry from
        a DataSourceComposable listing
    :param source DataSource implementation, if there's a special data source
    :param modify_transfer Transfer extension that identifies how to retrieve data from a source for modification of
        CAOM2 metadata. By this time, files are usually stored at CADC, so it's probably a CadcTransfer instance, but
        this allows for the case that a file is never stored at CADC. Try to guess what this one is.
    :param metadata_reader: MetadataReader instance
    :param state: bool True if using StateRunner
    :param store_transfer Transfer extension that identifies hot to retrieve data from a source for storage at CADC,
        probably an HTTP or FTP site. Don't try to guess what this one is.
    :param meta_visitors list of modules with visit methods, that expect the metadata of a work file to exist on disk
    :param data_visitors list of modules with visit methods, that expect the work file to exist on disk
    :param chooser OrganizerChooser, if there's rules that are unique to a collection about file naming.
    """
    if config is None:
        config = mc.Config()
        config.get_executors()

    _set_logging(config)
    logging.debug(
        f'Setting collection to {config.collection}, preview scheme to {config.preview_scheme} and scheme to '
        f'{config.scheme} in StorageName.'
    )
    mc.StorageName.collection = config.collection
    mc.StorageName.preview_scheme = config.preview_scheme
    mc.StorageName.scheme = config.scheme

    observable = mc.Observable(mc.Rejected(config.rejected_fqn), mc.Metrics(config))
    reporter = mc.ExecutionReporter(config, observable, application)
    reporter.set_log_location(config)
    if clients is None:
        clients = cc.ClientCollection(config)
    clients.metrics = observable.metrics
    if name_builder is None:
        name_builder = name_builder_composable.builder_factory(config)
    if metadata_reader is None:
        metadata_reader = reader_composable.reader_factory(config, clients)
    if source is None:
        source = data_source_composable.data_source_factory(config, clients, state, metadata_reader, reporter)
    else:
        source.reporter = reporter
    if modify_transfer is None:
        modify_transfer = transfer_composable.modify_transfer_factory(
            config, clients
        )

    if store_transfer is None:
        store_transfer = transfer_composable.store_transfer_factory(
            config, clients
        )

    organizer = ec.OrganizeExecutes(
        config,
        meta_visitors,
        data_visitors,
        chooser,
        store_transfer,
        modify_transfer,
        metadata_reader,
        clients,
        observable,
        reporter,
    )

    return (
        config,
        clients,
        name_builder,
        source,
        metadata_reader,
        organizer,
        observable,
        reporter,
    )


def run_by_todo(
    config=None,
    name_builder=None,
    chooser=None,
    source=None,
    meta_visitors=[],
    data_visitors=[],
    modify_transfer=None,
    store_transfer=None,
    clients=None,
    metadata_reader=None,
    application='DEFAULT',
):
    """A default implementation for using the TodoRunner.

    :param config Config instance
    :param name_builder NameBuilder extension that creates an instance of
        a StorageName extension, from an entry from a DataSourceComposable
        listing
    :param source DataSource implementation, if there's a special data source
    :param meta_visitors list of modules with visit methods, that expect
        the metadata of a work file to exist on disk
    :param data_visitors list of modules with visit methods, that expect the
        work file to exist on disk
    :param chooser OrganizerChooser, if there's strange rules about file
        naming.
    :param modify_transfer Transfer extension that identifies how to retrieve
        data from a source for modification of CAOM2 metadata. By this time,
        files are usually stored at CADC, so it's probably a CadcTransfer
        instance, but this allows for the case that a file is never stored
        at CADC. Try to guess what this one is.
    :param store_transfer Transfer extension that identifies hot to retrieve
        data from a source for storage at CADC, probably an HTTP or FTP site.
        Don't try to guess what this one is.
    :param clients: ClientCollection instance
    :param metadata_reader: MetadataReader instance
    :param application str Name for finding the version
    """
    (
        config,
        clients,
        name_builder,
        source,
        metadata_reader,
        organizer,
        observable,
        reporter,
    ) = common_runner_init(
        config,
        clients,
        name_builder,
        source,
        modify_transfer,
        metadata_reader,
        False,
        store_transfer,
        meta_visitors,
        data_visitors,
        chooser,
        application,
    )

    runner = TodoRunner(
        config, organizer, name_builder, source, metadata_reader, observable, reporter
    )
    result = runner.run()
    result |= runner.run_retry()
    runner.report()
    return result


def run_by_state(
    config=None,
    name_builder=None,
    bookmark_name=None,
    meta_visitors=[],
    data_visitors=[],
    end_time=None,
    chooser=None,
    source=None,
    modify_transfer=None,
    store_transfer=None,
    clients=None,
    metadata_reader=None,
    application='DEFAULT',
):
    """A default implementation for using the StateRunner.

    :param config Config instance
    :param name_builder NameBuilder extension that creates an instance of
        a StorageName extension, from an entry from a DataSourceComposable
        listing
    :param bookmark_name string that represents the state.yml lookup value
    :param meta_visitors list of modules with visit methods, that expect
        the metadata of a work file to exist on disk
    :param data_visitors list of modules with visit methods, that expect the
        work file to exist on disk
    :param end_time datetime for stopping a run, should be in UTC.
    :param chooser OrganizerChooser, if there's strange rules about file
        naming.
    :param source DataSourceComposable extension that identifies work to be
        done.
    :param modify_transfer Transfer extension that identifies how to retrieve
        data from a source for modification of CAOM2 metadata. By this time,
        files are usually stored at CADC, so it's probably a CadcTransfer
        instance, but this allows for the case that a file is never stored
        at CADC. Try to guess what this one is.
    :param store_transfer Transfer extension that identifies how to retrieve
        data from a source for storage at CADC, probably an HTTP or FTP site.
        Don't try to guess what this one is.
    :param clients instance of ClientsCollection, if one was required
    :param metadata_reader instance of MetadataReader
    :param application str Name for finding the version
    """
    (
        config,
        clients,
        name_builder,
        source,
        metadata_reader,
        organizer,
        observable,
        reporter,
    ) = common_runner_init(
        config,
        clients,
        name_builder,
        source,
        modify_transfer,
        metadata_reader,
        True,
        store_transfer,
        meta_visitors,
        data_visitors,
        chooser,
        application,
    )

    if end_time is None:
        end_time = get_utc_now_tz()

    runner = StateRunner(
        config,
        organizer,
        name_builder,
        source,
        metadata_reader,
        bookmark_name,
        observable,
        reporter,
        end_time,
    )
    result = runner.run()
    result |= runner.run_retry()
    runner.report()
    return result


def run_single(
    config,
    storage_name,
    meta_visitors,
    data_visitors,
    chooser=None,
    store_transfer=None,
    modify_transfer=None,
    metadata_reader=None,
    application='DEFAULT',
):
    """Process a single entry by StorageName detail.

    :param config mc.Config
    :param storage_name instance of StorageName for the collection
    :param meta_visitors List of metadata visit methods.
    :param data_visitors List of data visit methods.
    :param chooser OrganizeChooser instance for detailed CaomExecute
        descendant choices
    :param store_transfer Transfer extension that identifies hot to retrieve
        data from a source for storage at CADC, probably an HTTP or FTP site.
        Don't try to guess what this one is.
    :param modify_transfer Transfer extension that identifies how to retrieve
        data from a source for modification of CAOM2 metadata. By this time,
        files are usually stored at CADC, so it's probably a CadcTransfer
        instance, but this allows for the case that a file is never stored
        at CADC. Try to guess what this one is.
    :param metadata_reader MetadataReader instance
    :param application str Name for finding the version
    """
    logging.debug(f'Begin run_single {config.work_fqn}')
    observable = mc.Observable(mc.Rejected(config.rejected_fqn), mc.Metrics(config))
    reporter = mc.ExecutionReporter(config, observable, application)
    reporter.set_log_location(config)
    clients = cc.ClientCollection(config)
    clients.metrics = observable.metrics
    if modify_transfer is None:
        modify_transfer = transfer_composable.modify_transfer_factory(
            config, clients
        )
    if metadata_reader is None:
        metadata_reader = reader_composable.reader_factory(config, clients)
    organizer = ec.OrganizeExecutes(
        config,
        meta_visitors,
        data_visitors,
        chooser,
        store_transfer,
        modify_transfer,
        metadata_reader,
        clients,
        observable,
        reporter,
    )
    organizer.complete_record_count = 1
    organizer.choose()
    result = organizer.do_one(storage_name)
    logging.debug(f'run_single result is {result}')
    return result

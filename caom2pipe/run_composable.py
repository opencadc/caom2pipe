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
import traceback

from datetime import datetime

from caom2pipe import manage_composable as mc

__all__ = ['TodoRunner', 'StateRunner']


class TodoRunner(object):
    """
    This class brings together the mechanisms for identifying the
    lists of work to be done (DataSource extensions), and the mechanisms for
    translating a list of work into a collection-specific name
    (StorageNameBuilder extensions).
    """
    def __init__(self, config, organizer, builder, data_source, chooser=None):
        self._builder = builder
        self._data_source = data_source
        self._config = config
        self._organizer = organizer
        # the list of work to be done, containing whatever is returned from
        # the DataSource instance
        self._todo_list = []
        self._logger = logging.getLogger()

    def _build_todo_list(self):
        self._logger.debug('Begin _build_todo_list.')
        self._todo_list = self._data_source.get_work()
        self._organizer.complete_record_count = len(self._todo_list)
        self._logger.info(
            f'Processing {self._organizer.complete_record_count} '
            f'{self._organizer._command_name} records.')
        self._logger.debug('End _build_todo_list.')

    def _finish_run(self):
        mc.create_dir(self._config.log_file_directory)
        self._organizer.observable.rejected.persist_state()
        self._organizer.observable.metrics.capture()

    def _run_todo_list(self):
        self._logger.debug('Begin _run_todo_list.')
        result = 0
        for entry in self._todo_list:
            storage_name = self._builder.build(entry)
            if storage_name.is_valid():
                logging.info(f'Processing {storage_name}')
                result |= self._organizer.do_one(storage_name)
            else:
                logging.error('{} failed naming validation check.'.format(
                    storage_name.obs_id))
                self._organizer.capture_failure(storage_name.obs_id,
                                                storage_name.file_name,
                                                'Invalid name format.')
        self._finish_run()
        self._logger.debug('End _run_todo_list.')
        return result

    def run(self):
        self._logger.debug('Begin run.')
        self._build_todo_list()
        result = self._run_todo_list()
        self._logger.info(f'Done, processed {self._organizer.success_count} '
                          f'of {self._organizer.complete_record_count} '
                          f'correctly.')
        self._logger.debug('End run.')
        return result


class StateRunner(TodoRunner):

    def __init__(self, config, organizer, builder, data_source, bookmark_name):
        super(StateRunner, self).__init__(config, organizer, builder,
                                          data_source)
        self._bookmark_name = bookmark_name

    def run(self):
        logging.debug(f'Begin run {self._config.work_fqn}')
        if not os.path.exists(os.path.dirname(self._config.progress_fqn)):
            os.makedirs(os.path.dirname(self._config.progress_fqn))

        logger = logging.getLogger()
        logger.setLevel(self._config.logging_level)
        logging.debug(self._config)

        state = mc.State(self._config.state_fqn)
        start_time = state.get_bookmark(self._bookmark_name)
        end_time = datetime.fromtimestamp(self._data_source.max_ts_s)

        # make sure prev_exec_time is type datetime
        prev_exec_time = mc.increment_time(start_time, 0)
        exec_time = min(
            mc.increment_time(prev_exec_time, self._config.interval), end_time)

        logging.debug(
            'Starting at {}, ending at {}'.format(start_time, end_time))
        result = 0
        if prev_exec_time == end_time:
            logging.info(
                'Start time is the same as end time {}, stopping.'.format(
                    start_time))
            exec_time = prev_exec_time
        else:
            cumulative = 0
            result = 0
            while exec_time <= end_time:
                logging.info('Processing from {} to {}'.format(prev_exec_time,
                                                               exec_time))
                entries = self._data_source.todo(prev_exec_time, exec_time)
                if len(entries) > 0:
                    self._data_source.initialize()
                    logging.info('Processing {} entries.'.format(len(entries)))
                    self._organizer.complete_record_count = len(entries)
                    self._organizer.success_count = 0
                    for storage_name in entries:
                        try:
                            result |= self._organizer.do_one(storage_name)
                        except Exception as e:
                            self._organizer.capture_failure(
                                storage_name.obs_id, storage_name.file_name,
                                e=traceback.format_exc())
                            logging.info(
                                'Execution failed for {} with {}'.format(
                                    storage_name.file_name, e))
                            logging.debug(traceback.format_exc())
                            # then keep processing the rest of the lines in
                            # the file
                            result |= -1
                    self._finish_run()
                cumulative += len(entries)
                mc.record_progress(self._config, self._organizer._command_name,
                                   len(entries), cumulative, start_time)

                state.save_state(self._bookmark_name, exec_time)

                if exec_time == end_time:
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
                exec_time = min(
                    mc.increment_time(prev_exec_time, self._config.interval),
                    end_time)

            # return result, exec_time
        state.save_state(self._bookmark_name, exec_time)
        logging.info(
            'Done {}, saved state is {}'.format(self._organizer._command_name,
                                                exec_time))
        return result

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
import ray
import time

from ray.util import ActorPool

from caom2pipe.client_composable import declare_client
from caom2pipe.data_source_composable import DecompressionDataSource
from caom2pipe.execute_composable import OrganizeExecutes
from caom2pipe.manage_composable import Config, StorageName
from caom2pipe.name_builder_composable import GuessingBuilder
from caom2pipe.reader_composable import StorageClientReader
from caom2pipe.transfer_composable import CadcTransfer


def _set_logging(config):
    formatter = logging.Formatter(
        '%(asctime)s:%(levelname)-8s:%(name)-12s:%(lineno)-4d:%(message)s'
    )
    logging.error(logging.getLogger().handlers)
    for handler in logging.getLogger().handlers:
        handler.setLevel(config.logging_level)
        handler.setFormatter(formatter)


@ray.remote
class SessionActor:
    def __init__(self):
        StorageName.scheme = scheme
        StorageName.collection = config.collection
        self._client = declare_client(config)
        self._builder = GuessingBuilder(StorageName)
        self._store_transfer = CadcTransfer()
        self._metadata_reader = StorageClientReader(self._client)
        self._organizer = OrganizeExecutes(
            config, 
            [], 
            [],
            metadata_reader=self._metadata_reader,
            store_transfer=self._store_transfer,
            cadc_client=self._client,
        )

    def do_one(self, entry):
        # _set_logging(config)
        storage_name = self._builder.build(entry)
        self._organizer.choose(storage_name)
        return self._organizer.do_one(storage_name)


start = time.time()
# uncomment the following to get a glimpse at what ray is doing
# environ['RAY_LOG_TO_STDERR'] = '1'
ray.init()
config = Config()
config.get_executors()
# work_to_do = TodoFileDataSource(config).get_work()
scheme = 'gemini' if config.collection == 'GEMINI' else 'cadc'
work_to_do = DecompressionDataSource(
    config, config.collection, scheme
).get_work()
print(f'{len(work_to_do)} records to process')

MAX_ACTORS = 2
x = list()
for ii in range(MAX_ACTORS):
    y = SessionActor.remote()
    x.append(y)
actor_pool = ActorPool(x)

gen = actor_pool.map_unordered(
    lambda a, v: a.do_one.remote(v), work_to_do
)

success = 0
failure = 0
for gg in gen:
    if gg == 0:
        success += 1
    else:
        failure += 1
end = time.time()
print(f'per task overhead (ms) ={(end - start)*1000/len(work_to_do)}')
print(f'duration is {end-start}')
print(f'{success} successes')
print(f'{failure} failures')
print(f'{len(work_to_do)} total')


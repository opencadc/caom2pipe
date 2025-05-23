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
import os

from caom2 import SimpleObservation, Algorithm
from caom2pipe import execute_composable as ec
from caom2pipe import manage_composable as mc


THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
TEST_FILES_DIR = '/test_files'
TEST_OBS_FILE = os.path.join(TEST_DATA_DIR, 'test_obs_id.fits.xml')


class TStorageName(mc.StorageName):
    def __init__(self, obs_id=None, file_name=None, source_names=[]):
        obs_id = 'test_obs_id' if obs_id is None else obs_id
        file_name = 'test_file.fits.gz' if file_name is None else file_name
        source_names = ['/tmp/test_file.fits.gz'] if len(source_names) == 0 else source_names
        super().__init__(
            obs_id=obs_id,
            file_name=file_name,
            source_names=source_names,
        )
        self._destination_uris = ['cadc:TEST/test_file.fits']

    def is_valid(self):
        return True


class TChooser(ec.OrganizeChooser):
    def __init(self):
        super().__init__()

    def needs_delete(self):
        return True

    def use_compressed(self, ignore):
        return True


def mock_read(collection, obs_id):
    return SimpleObservation(
        collection=collection,
        observation_id=obs_id,
        algorithm=Algorithm('exposure'),
    )


def mock_get_file(collection, f_name, **kwargs):
    dest_fqn = kwargs.get('destination')
    with open(dest_fqn, 'w') as f:
        f.write(f'{collection} {f_name}\n')


def mock_copy(source, destination):
    with open(destination, 'w') as f:
        f.write('test content')
    return os.stat(destination).st_size


def mock_si_get(id, dest):
    mock_copy(id, dest)


def mock_copy_md5(source, destination, **kwargs):
    return mock_copy(source, destination)


def mock_get_node(uri, **kwargs):
    node = type('', (), {})()
    node.props = {'length': 42, 'MD5': '1234'}
    return node

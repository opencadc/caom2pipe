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

from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc

import test_conf as tc


def test_storage_name_builder():
    test_subject = nbc.StorageNameBuilder()
    test_storage_name = tc.TestStorageName()
    assert (
        test_subject.build(test_storage_name) == test_storage_name
    ), 'build wrong result'


def test_storage_name_instance_builder():
    test_config = mc.Config()
    test_config.collection = 'TEST_COLLECTION'
    test_subject = nbc.StorageNameInstanceBuilder(test_config)
    test_result = test_subject.build('test_storage_name.fits')
    assert test_result.obs_id == 'test_storage_name', 'wrong obs_id'
    assert test_result.collection == 'TEST_COLLECTION', 'wrong collection'
    assert test_result.fname_on_disk == 'test_storage_name.fits', 'wrong fname'


def test_guessing_builder():
    test_subject = nbc.GuessingBuilder(tc.TestStorageName)
    test_result = test_subject.build('test_storage_name.fits')
    # note TestStorageName has its own hard-coded values
    assert test_result.obs_id == 'test_obs_id', 'wrong obs_id'
    assert test_result.collection == 'TEST', 'wrong collection'
    assert test_result.fname_on_disk == 'test_file.fits.gz', 'wrong fname'
    assert test_result.entry == 'test_storage_name.fits', 'wrong entry'


def test_guessing_builder_dir():
    test_subject = nbc.GuessingBuilder(tc.TestStorageName)
    test_result = test_subject.build('/data/test_storage_name.fits')
    # note TestStorageName has its own hard-coded values
    assert test_result.obs_id == 'test_obs_id', 'wrong obs_id'
    assert test_result.collection == 'TEST', 'wrong collection'
    assert test_result.fname_on_disk == 'test_file.fits.gz', 'wrong fname'
    assert test_result.entry == '/data/test_storage_name.fits', 'wrong entry'
    assert (
        test_result.source_names == ['/data/test_storage_name.fits']
    ), 'wrong source names'
    assert (
        test_result.destination_uris == ['cadc:TEST/test_file.fits.gz']
    ), ' wrong destination uris'


def test_obs_id_builder():
    test_subject = nbc.ObsIDBuilder(tc.TestStorageName)
    test_result = test_subject.build('test_obs_id_2')
    # note TestStorageName has its own hard-coded values
    assert test_result.obs_id == 'test_obs_id', 'wrong obs_id'
    assert test_result.collection == 'TEST', 'wrong collection'
    assert test_result.fname_on_disk == 'test_file.fits.gz', 'wrong fname'
    assert test_result.entry == 'test_obs_id_2', 'wrong entry'


def test_guessing_builder_uri():
    test_subject = nbc.GuessingBuilder(tc.TestStorageName)
    test_result = test_subject.build(
        'https://localhost/data/test_storage_name.fits'
    )
    # note TestStorageName has its own hard-coded values
    assert test_result.obs_id == 'test_obs_id', 'wrong obs_id'
    assert test_result.collection == 'TEST', 'wrong collection'
    assert (
        test_result.entry == 'https://localhost/data/test_storage_name.fits'
    ), 'wrong entry'
    assert (
            test_result.source_names ==
            ['https://localhost/data/test_storage_name.fits']
    ), 'wrong source names'
    assert (
            test_result.destination_uris == ['cadc:TEST/test_file.fits.gz']
    ), ' wrong destination uris'

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

import os
import pytest
import shutil

no_footprintfinder = False
from astropy.table import Table
from caom2 import ValueCoord2D
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc

from mock import Mock, patch
import test_conf as tc

try:
    import footprintfinder
    no_footprintfinder = False
except ImportError:
    no_footprintfinder = True


@pytest.mark.skipif(
    no_footprintfinder, reason='footprintfinder must be installed'
)
def test_exec_footprintfinder():
    test_obs_file = 'fpf_start_obs.xml'
    test_obs = mc.read_obs_from_file(os.path.join(
        tc.TEST_DATA_DIR, test_obs_file))
    test_chunk = \
        test_obs.planes[
            'VLASS1.2.T07t14.J084202-123000.quicklook.v1'
        ].artifacts[
            'ad:VLASS/VLASS1.2.ql.T07t14.J084202-123000.10.2048.v1.I.iter1.'
            'image.pbcor.tt0.subim.fits'
        ].parts[
            '0'
        ].chunks.pop()
    test_file_id = 'VLASS1.2.ql.T24t07.J065836+563000.10.2048.v1.I.iter1.' \
                   'image.pbcor.tt0.subim'
    test_file = os.path.join(tc.TEST_FILES_DIR, f'{test_file_id}.fits')
    if not os.path.exists(test_file):
        shutil.copy(f'/usr/src/app/vlass2caom2/int_test/test_files/'
                    f'{test_file_id}.fits', test_file)
    test_log_dir = os.path.join(tc.TEST_DATA_DIR, 'logs')
    assert test_chunk is not None, 'chunk expected'
    assert test_chunk.position is not None, 'position expected'
    assert test_chunk.position.axis is not None, 'axis expected'
    assert test_chunk.position.axis.bounds is None, 'bounds not expected'

    cc.exec_footprintfinder(
        test_chunk, test_file, test_log_dir, test_file_id, '-t 10'
    )
    assert test_chunk is not None, 'chunk unchanged'
    assert test_chunk.position is not None, 'position unchanged'
    assert test_chunk.position.axis is not None, 'axis unchanged'
    assert test_chunk.position.axis.bounds is not None, 'bounds expected'
    assert (
        len(test_chunk.position.axis.bounds.vertices) == 17
    ), 'wrong number of vertices'
    assert (
        test_chunk.position.axis.bounds.vertices[0] ==
        ValueCoord2D(
            coord1=105.188421, coord2=55.98216
        )
    ), 'wrong first vertex'
    assert (
        test_chunk.position.axis.bounds.vertices[16] ==
        ValueCoord2D(
            coord1=105.165491, coord2=56.050318
        )
    ), 'wrong last vertex'

    if os.path.exists(test_file):
        os.unlink(test_file)


def test_reset():
    test_obs_file = 'fpf_start_obs.xml'
    test_obs = mc.read_obs_from_file(
        os.path.join(tc.TEST_DATA_DIR, test_obs_file)
    )
    test_chunk = \
        test_obs.planes[
            'VLASS1.2.T07t14.J084202-123000.quicklook.v1'
        ].artifacts[
            'ad:VLASS/VLASS1.2.ql.T07t14.J084202-123000.10.2048.v1.I.iter1.'
            'image.pbcor.tt0.subim.fits'
        ].parts[
            '0'
        ].chunks.pop()

    assert test_chunk is not None, 'chunk expected'
    assert test_chunk.position is not None, 'position expected'
    assert test_chunk.position.axis is not None, 'axis expected'
    assert test_chunk.position.axis.bounds is None, 'bounds not expected'
    assert test_chunk.energy is not None, 'energy expected'
    assert test_chunk.energy_axis is not None, 'energy axis expected'

    cc.reset_position(test_chunk)
    assert test_chunk.position is None, 'position not expected'
    assert test_chunk.position_axis_1 is None, 'axis 1 not expected'
    assert test_chunk.position_axis_2 is None, 'axis 2 not expected'

    cc.reset_energy(test_chunk)
    assert test_chunk.energy is None, 'energy not expected'
    assert test_chunk.energy_axis is None, 'energy axis not expected'

    cc.reset_observable(test_chunk)
    assert test_chunk.observable is None, 'observable not expected'
    assert test_chunk.observable_axis is None, 'observable axis not expected'


@patch('caom2pipe.client_composable.query_tap_client')
def test_build_temporal_wcs(query_mock):
    def _mock_query(arg1, arg2):
        if 'N20160102S0296' in arg1:
            return Table.read(
                'val,delta,cunit,naxis\n'
                '57389.66314699074,0.000115798611111111,d,1\n'.split('\n'),
                format='csv'
            )
        else:
            return Table.read(
                'val,delta,cunit,naxis\n'
                '57389.66342476852,0.000115798611111111,d,1\n'.split('\n'),
                format='csv'
            )

    query_mock.side_effect = _mock_query

    test_tap_client = Mock()
    test_observation = mc.read_obs_from_file(
        f'{tc.TEST_DATA_DIR}/build_temporal_wcs_start.xml'
    )
    test_plane = test_observation.planes['GN2001BQ013-04']
    test_part = test_plane.artifacts['cadc:GEMINI/test.fits'].parts['0']
    assert test_part.chunks[0].time is None, 'temporal wcs ic'
    test_collection = 'TEST'
    cc.build_temporal_wcs_bounds(
        test_tap_client, test_plane, test_collection
    )
    assert test_part.chunks[0].time is not None, 'temporal wcs ic change'
    test_result = test_part.chunks[0].time
    assert test_result.axis is not None, 'expect axis'
    assert test_result.axis.bounds is not None, 'expect bounds'
    assert len(test_result.axis.bounds.samples) == 2, 'expect two samples'

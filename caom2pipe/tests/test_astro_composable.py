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

import math

from unittest.mock import patch
import test_conf as tc

from astropy.io import fits
from astropy.io.votable import parse_single_table

from caom2pipe import astro_composable as ac


def test_convert_time():
    hdr1 = fits.Header()
    mjd_start, mjd_end = ac.find_time_bounds([hdr1])
    assert mjd_start is None
    assert mjd_end is None

    hdr1['DATE-OBS'] = '2012-09-03T01:04:44'
    hdr1['TEXP'] = 20.000
    mjd_start, mjd_end = ac.find_time_bounds([hdr1])
    assert mjd_start is not None
    assert mjd_end is not None
    assert math.isclose(mjd_start, 56173.044953703706), mjd_start
    assert math.isclose(mjd_end, 56173.04518518518), mjd_end


def test_get_datetime():
    result = ac.get_datetime('2006-12-12T12:12:12')
    assert result is not None
    assert result == '2006-12-12 12:12:12.000'

    result = ac.get_datetime('2006-12-12 12:12:12.001')
    assert result is not None
    assert result == '2006-12-12 12:12:12.001'

    result = ac.get_datetime('2006-12-12')
    assert result is not None
    assert result == '2006-12-12 00:00:00.000'

    # a format that is not understood
    result = ac.get_datetime('16-Dec-12T01:23:45')
    assert result is None

    result = ac.get_datetime(None)
    assert result is None


def test_get_location():
    x, y, z = ac.get_location(21.0, -32.0, 12)
    assert x == 5051887.288718968, x
    assert y == -3156769.5360207916, y
    assert z == 2271399.319625149, z


def test_build_plane_time():
    start = ac.get_datetime('2012-09-03T01:04:44')
    end = ac.get_datetime('2012-09-03T03:04:44')
    exposure = end - start
    result = ac.build_plane_time(start, end, exposure)
    assert result is not None, 'expected a value'
    assert result.bounds is not None, 'expected a bounds value'
    assert math.isclose(
        result.exposure, 7199.999999999994
    ), 'wrong exposure value'


def test_get_time_delta_in_s():
    result = ac.get_timedelta_in_s('0:06:41')
    assert result is not None
    assert result == 401, 'wrong value returned'


def test_build_ra_dec_as_deg():
    test_ra = '18:51:46.723100'
    test_dec = '+00:35:32.36300'
    result_ra, result_dec = ac.build_ra_dec_as_deg(test_ra, test_dec)
    assert result_ra is not None
    assert math.isclose(result_ra, 282.9446795833333), 'wrong ra value'
    assert result_dec is not None
    assert math.isclose(result_dec, 0.5923230555555556), 'wrong dec value'


@patch('caom2pipe.astro_composable.get_vo_table')
def test_filter_md_cache(query_mock):
    call_count = 0

    def _vo_mock(url):
        result = None
        error_message = None
        table = None
        if 'unrepaired_inst.unrepaired_fn' in url:
            table = f'{tc.TEST_DATA_DIR}/votable/uncached.xml'
            nonlocal call_count
            call_count += 1
        else:
            error_message = f'Do not understand url {url}'
        if table is not None:
            result = parse_single_table(table)
        return result, error_message

    query_mock.side_effect = _vo_mock

    fn_repair = {'collection': 'repaired'}
    inst_repair = {'collection': 'repaired'}
    test_cache = {'repaired': {'cw': 4444.2043, 'fwhm': 333.9806}}
    test_subject = ac.FilterMetadataCache(
        fn_repair, inst_repair, telescope=None, cache=test_cache
    )
    assert 'unrepaired_fn' not in test_subject._cache, 'cache broken'

    # uncached, unrepaired
    test_result = test_subject.get_svo_filter(
        'unrepaired_inst', 'unrepaired_fn'
    )
    assert test_result is not None, 'expect a result'
    assert (
        ac.FilterMetadataCache.get_central_wavelength(test_result)
        == 3740.2043404255
    ), 'wrong cw'
    assert (
        ac.FilterMetadataCache.get_fwhm(test_result) == 414.98068085106
    ), 'wrong fwhm'
    assert call_count == 1, 'wrong execution path'
    assert (
        'unrepaired_inst.unrepaired_fn' in test_subject._cache
    ), 'cache broken'

    # cached, unrepaired
    test_result = test_subject.get_svo_filter(
        'unrepaired_inst', 'unrepaired_fn'
    )
    assert test_result is not None, 'expect a result'
    assert (
        ac.FilterMetadataCache.get_central_wavelength(test_result)
        == 3740.2043404255
    ), 'wrong cw'
    assert (
        ac.FilterMetadataCache.get_fwhm(test_result) == 414.98068085106
    ), 'wrong fwhm'
    assert call_count == 1, 'wrong execution path'

    # uncached, repaired
    test_result = test_subject.get_svo_filter('collection', 'collection')
    assert test_result is not None, 'expect a result'
    assert (
        ac.FilterMetadataCache.get_central_wavelength(test_result)
        == 4444.2043
    ), 'wrong cw'
    assert (
        ac.FilterMetadataCache.get_fwhm(test_result) == 333.9806
    ), 'wrong fwhm'

    # unfound
    test_result = test_subject.get_svo_filter('undefined', 'undefined')
    assert test_result is not None, 'expect result'
    assert (
        ac.FilterMetadataCache.get_central_wavelength(test_result) == -2
    ), 'wrong cw'
    assert ac.FilterMetadataCache.get_fwhm(test_result) == -2, 'wrong fwhm'


def test_check_h5():
    test_fqn_success = '/test_files/2384125z.hdf5'
    test_result = ac.check_h5(test_fqn_success)
    assert test_result, 'expect success'

    test_fqn_empty = '/test_files/empty.h5'
    test_result = ac.check_h5(test_fqn_empty)
    assert not test_result, 'empty failure'

    test_fqn_broken = '/test_files/broken.h5'
    test_result = ac.check_h5(test_fqn_broken)
    assert not test_result, 'broken failure'

    test_fqn_missing = '/test_files/not_there.h5'
    test_result = ac.check_h5(test_fqn_missing)
    assert not test_result, 'missing failure'


def test_check_fitsverify():
    files = {
        '/test_files/a2020_06_17_07_00_01.fits': True,
        '/test_files/a2022_07_26_05_50_01.fits': False,
        '/test_files/2377897o.fits.fz': True,
        '/test_files/scatsmth.flat.V.00.01.fits.gz': False,
        '/test_files/NEOS_SCI_2022223000524.fits': True,
        '/test_files/broken.fits': False,
        '/test_files/dao_c122_2007_000882_v_1024.png': False,
    }

    for fqn, expected_result in files.items():
        test_result = ac.check_fitsverify(fqn)
        assert test_result == expected_result, f'wrong fitsverify result {fqn}'

    files = {
        '/test_files/a2020_06_17_07_00_01.fits': True,
        '/test_files/a2022_07_26_05_50_01.fits': True,
        '/test_files/2377897o.fits.fz': True,
        '/test_files/scatsmth.flat.V.00.01.fits.gz': True,
        '/test_files/NEOS_SCI_2022223000524.fits': True,
        '/test_files/broken.fits': False,
        '/test_files/dao_c122_2007_000882_v_1024.png': False,
    }

    for fqn, expected_result in files.items():
        test_result = ac.check_fits(fqn)
        assert test_result == expected_result, f'wrong astropy verify result {fqn}'

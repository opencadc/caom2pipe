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

import io
import logging
import requests

from astropy import units
from astropy.io import fits
from astropy.io.votable import parse_single_table
from astropy.coordinates import EarthLocation
from astropy.time import Time, TimeDelta
from astropy.coordinates import SkyCoord

from datetime import timedelta as dt_timedelta
from datetime import datetime as dt_datetime
from enum import Enum
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from time import strptime as dt_strptime

from caom2 import Interval as caom_Interval
from caom2 import Time as caom_Time
from caom2 import shape as caom_shape
from caom2 import CoordBounds1D, CoordRange1D, RefCoord

from caom2pipe import manage_composable as mc


__all__ = ['convert_time', 'get_datetime', 'build_chunk_energy_bounds',
           'build_plane_time', 'build_plane_time_interval',
           'build_plane_time_sample', 'build_ra_dec_as_deg',
           'get_geocentric_location', 'get_location', 'get_timedelta_in_s',
           'make_headers_from_string', 'get_vo_table', 'read_fits_data',
           'read_fits_headers', 'SVO_URL', 'FilterMetadataCache']

SVO_URL = 'http://svo2.cab.inta-csic.es/svo/theory/fps3/fps.php?ID='


def find_time_bounds(headers):
    """Given an observation date, and time exposure length, calculate the
    mjd_start and mjd_end time for those values."""
    logging.debug('Begin find_time_bounds.')
    date = headers[0].get('DATE-OBS')
    exposure = headers[0].get('TEXP')
    return convert_time(date, exposure)


def convert_time(start_time, exposure):
    """Convert a start time and exposure length into an mjd_start and mjd_end
    time."""
    logging.debug('Begin convert_time.')
    if start_time is not None and exposure is not None:
        logging.debug(
            'Use date {} and exposure {} to convert time.'.format(start_time,
                                                                  exposure))
        if type(start_time) is float:
            t_start = Time(start_time, format='mjd')
        else:
            t_start = Time(start_time)
        dt = TimeDelta(exposure, format='sec')
        t_end = t_start + dt
        t_start.format = 'mjd'
        t_end.format = 'mjd'
        mjd_start = t_start.value
        mjd_end = t_end.value
        logging.debug('End convert_time mjd start {} mjd end {} .'.format(
            mjd_start, mjd_end))
        return mjd_start, mjd_end
    return None, None


def get_datetime(from_value):
    """
    Ensure datetime values are in MJD. This is meant to handle any odd formats
    that telescopes have for datetime values.

    Relies on astropy, until astropy fails.

    :param from_value:
    :return: datetime instance
    """
    result = None
    if from_value is not None:
        import numpy
        # local import, in case the container is not provisioned with
        # numpy
        if isinstance(from_value, str):
            try:
                result = Time(from_value)
            except ValueError:
                # VLASS has a format astropy fails to understand '%H:%M:%S'
                # CFHT 2019/11/26
                # Gemini 2019-11-01 00:01:34.610517+00:00
                if '+00:00' in from_value:
                    # because %z doesn't expect the ':' in the timezone field
                    from_value = from_value[:-6]
                for fmt in ['%H:%M:%S', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S.%f']:
                    try:
                        result = Time(dt_datetime.strptime(from_value, fmt))
                        break
                    except ValueError:
                        pass
        elif isinstance(from_value, numpy.int32):
            result = Time(dt_datetime.fromtimestamp(from_value))
    if result is None:
        logging.error('Cannot parse datetime {}'.format(from_value))
    else:
        result.format = 'mjd'
    return result


def get_geocentric_location(site):
    """Rely on astropy to provide the geocentric location of known
    telescopes."""
    result = EarthLocation.of_site(site)
    return result.x.value, result.y.value, result.z.value


def get_location(latitude, longitude, elevation):
    """The CAOM model expects the telescope location to be in geocentric
    coordinates. Rely on astropy to do the conversion."""
    result = EarthLocation.from_geodetic(
        longitude, latitude, elevation, 'WGS84')
    return result.x.value, result.y.value, result.z.value


def get_vo_table(url):
    """
    Download the VOTable XML for the given url and return a astropy.io.votable
    object.

    :param url: query url for the SVO service
    :return: astropy.io.votable of the first table and
             an error_message if there was an error downloading the data
    """
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    vo_table = None
    response = None
    error_message = None
    try:
        response = session.get(url)
        fh = io.BytesIO(bytes(response.text, 'utf-8'))
        response.close()
        vo_table = parse_single_table(fh)
    except Exception as e:
        error_message = str(e)
    if response:
        response.close()
    return vo_table, error_message


def build_chunk_energy_bounds(wave, axis):
    import numpy as np  # limit the  effect on container content
    # caom2IngestEspadons.py, l698
    x = np.arange(1, wave.size + 1, dtype='float32')
    wavegrade = np.gradient(wave)
    waveinflect = wave[np.where(abs(wavegrade) > 0.01)]
    xinflect = x[np.where(abs(wavegrade) > 0.01)]
    # add start and finish pixels onto waveinflect and xinflect and these are
    # our list of sub-bounds.
    allxinflect = np.append(x[0], xinflect)
    allxinflect = np.append(allxinflect, x[-1])
    allwaveinflect = np.append(wave[0], waveinflect)
    allwaveinflect = np.append(allwaveinflect, wave[-1])
    numwaveschunk = int(len(allxinflect) / 2.0)

    bounds = CoordBounds1D()
    for jj in range(numwaveschunk):
        indexlo = jj * 2 + 0
        indexhi = indexlo + 1
        x1 = float(allxinflect[indexlo])
        x2 = float(allxinflect[indexhi])
        w1 = float(allwaveinflect[indexlo])
        w2 = float(allwaveinflect[indexhi])
        coord_range = CoordRange1D(RefCoord(x1, w1), RefCoord(x2, w2))
        bounds.samples.append(coord_range)

    return bounds


def build_plane_time(start_date, end_date, exposure_time):
    """Calculate the plane-level bounding box for time, with one sample."""
    sample = build_plane_time_sample(start_date, end_date)
    time_bounds = build_plane_time_interval(start_date, end_date, [sample])
    return caom_Time(bounds=time_bounds,
                     dimension=1,
                     resolution=exposure_time.to('second').value,
                     sample_size=exposure_time.to('day').value,
                     exposure=exposure_time.to('second').value)


def build_plane_time_interval(start_date, end_date, samples):
    """Create an Interval for the plane-level bounding box for time, given
    the start and end dates, and a list of samples.
    :param samples list of SubInterval instances
    :param start_date minimum SubInterval date
    :param end_date maximum SubInterval date. """
    time_bounds = caom_Interval(mc.to_float(start_date.value),
                                mc.to_float(end_date.value),
                                samples=samples)
    return time_bounds


def build_plane_time_sample(start_date, end_date):
    """Create a SubInterval for the plane-level bounding box for time, given
    the start and end dates.
    :param start_date minimum date
    :param end_date maximum date. """
    start_date.format = 'mjd'
    end_date.format = 'mjd'
    return caom_shape.SubInterval(
        mc.to_float(start_date.value),
        mc.to_float(end_date.value))


def build_ra_dec_as_deg(ra, dec, frame='icrs'):
    """
    Common code to go from units.hourangle, units.deg to both values in
    units.deg
    """
    result = SkyCoord(ra, dec, frame=frame,
                      unit=(units.hourangle, units.deg))
    return result.ra.degree, result.dec.degree


def get_timedelta_in_s(from_value):
    """
    :param from_value: a string representing time in H:M:S
    :return: the value as a timedelta, in seconds
    """
    temp = dt_strptime(from_value, '%H:%M:%S')
    td = dt_timedelta(
        hours=temp.tm_hour, minutes=temp.tm_min, seconds=temp.tm_sec)
    return td.seconds


def make_headers_from_string(fits_header):
    """Create a list of fits.Header instances from a string.
    ":param fits_header a string of keyword/value pairs"""
    delim = '\nEND'
    extensions = \
        [e + delim for e in fits_header.split(delim) if e.strip()]
    headers = [fits.Header.fromstring(e, sep='\n') for e in extensions]
    return headers


def read_fits_data(fqn):
    """Read a complete fits file, including the data.
    :param fqn a string representing the fully-qualified name of the fits
        file.
    :return fits file content. The client needs to call close on the content.
    """
    hdus = fits.open(fqn, memmap=True, lazy_load_hdus=False)
    return hdus


def read_fits_headers(fqn):
    """Read the headers from a fits file.
    :param fqn a string representing the fully-qualified name of the fits
        file.
    :return fits file headers.
    """
    hdulist = fits.open(fqn, memmap=True, lazy_load_hdus=False)
    hdulist.close()
    headers = [h.header for h in hdulist]
    return headers


class FilterMetadataCache(object):
    """
    Cache the results of calls to the SVO filter service. As part of the
    caching, identify those filters that are not available from the service,
    so there is no continual asking for something that doesn't exist.

    Units are Angstroms.
    """

    def __init__(self, repair_filter_lookup, repair_instrument_lookup,
                 telescope, cache=None, default_key='NONE',
                 connected=True):
        # a dict
        # key - the collection filter name
        # value - the filter name as used at SVO
        self._repair_filter = repair_filter_lookup
        self._repair_instrument = repair_instrument_lookup
        self._telescope = telescope
        self._cache = cache
        # which value to use, when there is no lookup information
        self._default_key = default_key
        # a way to work with the TaskType.SCRAPE - False means no
        # external connection, so the SVO query will fail, so don't try
        self._connected = connected
        self._logger = logging.getLogger()

    def _repair_filter_name(self, coll_filter_name, instrument):
        result = self._repair_filter.get(coll_filter_name, coll_filter_name)
        if result is None or result in ['NONE', 'Open']:
            result = f'{instrument}.None'
        return result

    def _repair_instrument_name(self, instrument):
        return self._repair_instrument.get(instrument, instrument)

    @property
    def connected(self):
        return self._connected

    @connected.setter
    def connected(self, value):
        self._connected = value

    def get_svo_filter(self, instrument, filter_name):
        """
        Return FWHM and WavelengthCen from SVO for named instrument/filter
        combinations.
        :return: units are Angstroms
        """
        if self._connected:
            if ((instrument is None or
                 (isinstance(instrument, Enum) and
                  instrument.value is None)) and filter_name is None):
                result = self._cache.get(self._default_key)
            else:

                inst_r = self._repair_instrument_name(instrument)
                fn_r = self._repair_filter_name(filter_name, inst_r)
                self._logger.debug(f'Looking for instrument {instrument}, '
                                   f'repaired instrument {inst_r}, filter '
                                   f'{filter_name} repaired filter {fn_r}.')
                result = self._cache.get(fn_r)
                if result is None:
                    central_wl = None
                    fwhm = None
                    # VERB=0 means return the smalled amount of filter metadata
                    url = f'{SVO_URL}{self._telescope}/{inst_r}.{fn_r}&VERB=0'
                    self._logger.info(f'Query for filter information: {url}')
                    vo_table, error_message = get_vo_table(url)
                    if vo_table is None:
                        self._logger.warning(
                            f'Unable to download SVO filter information '
                            f'from {url} because {error_message}')
                    else:
                        fwhm = vo_table.get_field_by_id('FWHM').value
                        central_wl = vo_table.get_field_by_id(
                            'WavelengthCen').value
                    result = {'cw': central_wl, 'fwhm': fwhm}
                    self._cache[fn_r] = result
        else:
            # some recognizably wrong value
            logging.warning(f'Not connected - using default energy values for '
                            f'{instrument} and {filter_name}')
            result = {'cw': -0.1, 'fwhm': -0.1}
        return result

    @staticmethod
    def get_central_wavelength(energy):
        result = None
        if energy is not None:
            result = energy.get('cw')
        return result

    @staticmethod
    def get_fwhm(energy):
        result = None
        if energy is not None:
            result = energy.get('fwhm')
        return result

    @staticmethod
    def get_resolving_power(energy):
        cw = FilterMetadataCache.get_central_wavelength(energy)
        fwhm = FilterMetadataCache.get_fwhm(energy)
        return cw / fwhm

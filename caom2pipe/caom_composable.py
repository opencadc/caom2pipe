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

from caom2 import CoordAxis1D, Axis, RefCoord, CoordRange1D, SpectralWCS
from caom2 import TypedSet, ObservationURI, PlaneURI, Chunk, CoordPolygon2D
from caom2 import ValueCoord2D, CompositeObservation, Algorithm, Artifact
from caom2.diff import get_differences

from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc

__all__ = ['exec_footprintfinder', 'update_plane_provenance',
           'update_observation_members', 'reset_energy', 'reset_position',
           'reset_observable', 'is_composite', 'change_to_composite',
           'compare']


def build_chunk_energy_range(chunk, filter_name, filter_md):
    """
    Set a range axis for chunk energy using central wavelength and FWHM.
    Units are Angstroms. Axis is set to 4.

    :param chunk: Chunk to add a CoordRange1D Axis to
    :param filter_name: string to set to bandpassName
    :param filter_md: dict with a 'cw' and 'fwhm' value
    """
    # If n_axis=1 (as I guess it will be for all but processes GRACES
    # spectra now?) that means crpix=0.5 and the corresponding crval would
    # central_wl - bandpass/2.0 (i.e. the minimum wavelength).   It is fine
    # if you instead change crpix to 1.0.   I guess since the ‘range’ of
    # one pixel is 0.5 to 1.5.

    cw = ac.FilterMetadataCache.get_central_wavelength(filter_md)
    fwhm = ac.FilterMetadataCache.get_fwhm(filter_md)
    if cw is not None and fwhm is not None:
        resolving_power = ac.FilterMetadataCache.get_resolving_power(filter_md)
        axis = CoordAxis1D(axis=Axis(ctype='WAVE', cunit='A'))
        ref_coord1 = RefCoord(0.5, cw - fwhm / 2.0)
        ref_coord2 = RefCoord(1.5, cw + fwhm / 2.0)
        axis.range = CoordRange1D(ref_coord1, ref_coord2)

        energy = SpectralWCS(axis=axis,
                             specsys='TOPOCENT',
                             ssyssrc=None,
                             ssysobs=None,
                             bandpass_name=filter_name,
                             resolving_power=resolving_power)
        chunk.energy = energy
        # PD - in general, do not set the energy_axis, unless the energy axis
        # was really in the fits header


def change_to_composite(observation, algorithm_name='composite',
                        collection=None, features=None):
    """For the case where a SimpleObservation needs to become a
    DerivedObservation."""
    if (collection is None or collection != 'CFHT' or
            (features is not None and not features.supports_latest_caom)):
        return CompositeObservation(observation.collection,
                                    observation.observation_id,
                                    Algorithm(algorithm_name),
                                    observation.sequence_number,
                                    observation.intent,
                                    observation.type,
                                    observation.proposal,
                                    observation.telescope,
                                    observation.instrument,
                                    observation.target,
                                    observation.meta_release,
                                    observation.planes,
                                    observation.environment,
                                    observation.target_position)
    else:
        from caom2 import DerivedObservation
        temp = DerivedObservation(observation.collection,
                                  observation.observation_id,
                                  Algorithm(algorithm_name),
                                  observation.sequence_number,
                                  observation.intent,
                                  observation.type,
                                  observation.proposal,
                                  observation.telescope,
                                  observation.instrument,
                                  observation.target,
                                  observation.meta_release,
                                  observation.meta_read_groups,
                                  observation.planes,
                                  observation.environment,
                                  observation.target_position)
        temp.meta_producer = observation.meta_producer
        return temp


def compare(ex_fqn, act_fqn, entry):
    """Run get_differences for two files on disk.

    :param ex_fqn: Fully-qualified file name for the expected observation
    :param act_fqn: Fully-qualified file name for the actual observation
    :param entry: String for logging information.
    :return:
    """
    ex = mc.read_obs_from_file(ex_fqn)
    act = mc.read_obs_from_file(act_fqn)
    result = get_differences(ex, act, 'Observation')
    if result:
        result_str = '\n'.join([r for r in result])
        msg = f'Differences found obs id {ex.observation_id} ' \
              f'file id {entry} ' \
              f'instr {ex.instrument.name}\n{result_str}'
        return msg
    return None


def copy_artifact(from_artifact, features=None):
    """Make a copy of an artifact. This works around the CAOM2 constraint
    'org.postgresql.util.PSQLException: ERROR: duplicate key value violates
    unique constraint "artifact_pkey"', when trying to use the same artifact
    information in different planes (e.g. when referring to the same
    thumbnail and preview files.

    :param from_artifact Artifact of which to make a shallow copy
    :param features which version of CAOM to use
    :return a copy of the from_artifact, with parts set to None
    """
    if features is not None and features.supports_latest_caom:
        copy = Artifact(uri=from_artifact.uri,
                        product_type=from_artifact.product_type,
                        release_type=from_artifact.release_type,
                        content_type=from_artifact.content_type,
                        content_length=from_artifact.content_length,
                        content_checksum=from_artifact.content_checksum,
                        content_release=from_artifact.content_release,
                        content_read_groups=from_artifact.content_read_groups,
                        parts=None)
    else:
        copy = Artifact(uri=from_artifact.uri,
                        product_type=from_artifact.product_type,
                        release_type=from_artifact.release_type,
                        content_type=from_artifact.content_type,
                        content_length=from_artifact.content_length,
                        content_checksum=from_artifact.content_checksum,
                        parts=None)
    return copy


def exec_footprintfinder(chunk, science_fqn, log_file_directory, obs_id,
                         params='-f'):
    """Execute the footprintfinder on a file. All preconditions for successful
    execution should be in place i.e. the file exists, and is unzipped (because
    that is faster).

    :param chunk The CAOM Chunk that will have Position Bounds information
        added
    :param science_fqn A string of the fully-qualified file name for
        footprintfinder to run on
    :param log_file_directory A string of the fully-qualified name for the log
        directory, where footprintfinder output files will be moved to, after
        execution
    :param obs_id specifies location where footprintfinder log files end up
    :param params specific footprintfinder parameters by collection - default
        forces full-chip, regardless of illumination
    """
    logging.debug('Begin _update_position')
    mc.check_param(chunk, Chunk)

    # local import because footprintfinder depends on matplotlib being
    # installed, which is not declared as a caom2pipe dependency
    import footprintfinder

    if (chunk.position is not None
            and chunk.position.axis is not None):
        logging.debug('position exists, calculate footprints for {}.'.format(
            science_fqn))
        full_area, footprint_xc, footprint_yc, ra_bary, dec_bary, \
            footprintstring, stc = footprintfinder.main(
                '-r {} {}'.format(params, science_fqn))
        logging.debug('footprintfinder result: full area {} '
                      'footprint xc {} footprint yc {} ra bary {} '
                      'dec_bary {} footprintstring {} stc {}'.format(
                          full_area, footprint_xc, footprint_yc, ra_bary,
                          dec_bary, footprintstring, stc))
        bounds = CoordPolygon2D()
        coords = None
        fp_results = stc.split('Polygon FK5')
        if len(fp_results) > 1:
            coords = fp_results[1].split()
        else:
            fp_results = stc.split('Polygon ICRS')
            if len(fp_results) > 1:
                coords = fp_results[1].split()

        if coords is None:
            raise mc.CadcException(
                'Do not recognize footprint {}'.format(stc))

        index = 0
        while index < len(coords):
            vertex = ValueCoord2D(mc.to_float(coords[index]),
                                  mc.to_float(coords[index + 1]))
            bounds.vertices.append(vertex)
            index += 2
            logging.debug('Adding vertex\n{}'.format(vertex))
        chunk.position.axis.bounds = bounds

        return_file = '{}_footprint.txt'.format(obs_id)
        return_string_file = '{}_footprint_returnstring.txt'.format(obs_id)
        _handle_footprint_logs(log_file_directory, return_file)
        _handle_footprint_logs(log_file_directory, return_string_file)

    else:
        logging.info('No position information for footprint generation.')
    logging.debug('Done _update_position.')


def _handle_footprint_logs(log_file_directory, log_file):
    """Move footprintfinder logs to specific log directory, if there
    is one."""
    orig_log_fqn = os.path.join(os.getcwd(), log_file)
    if log_file_directory is not None and os.path.exists(log_file_directory):
        if os.path.exists(orig_log_fqn):
            log_fqn = os.path.join(log_file_directory, log_file)
            os.rename(orig_log_fqn, log_fqn)
            logging.debug('Moving footprint log file from {} to {}'.format(
                orig_log_fqn, log_fqn))
    else:
        logging.debug('Removing footprint log file {}'.format(orig_log_fqn))
        os.unlink(orig_log_fqn)


def is_composite(headers, keyword_prefix='IMCMB'):
    """All the logic to determine if a file name is part of a
    CompositeObservation, in one marvelous function."""
    result = False

    # look in the last header - IMCMB keywords are not in the zero'th header
    header = headers[-1]
    for ii in header:
        if ii.startswith(keyword_prefix):
            result = True
            break
    return result


def update_plane_provenance(plane, headers, lookup, collection,
                            repair, obs_id):
    """Add inputs to Planes, based on a particular keyword prefix.

    :param plane Plane instance to add inputs to
    :param headers FITS keyword headers that have lookup values.
    :param lookup The keyword pattern to find in the FITS header keywords for
        input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    """
    plane_inputs = TypedSet(PlaneURI,)

    for header in headers:
        for keyword in header:
            if keyword.startswith(lookup):
                value = header.get(keyword)
                prov_obs_id, prov_prod_id = repair(value, obs_id)
                if prov_obs_id is not None and prov_prod_id is not None:
                    obs_member_uri_str = \
                        mc.CaomName.make_obs_uri_from_obs_id(
                            collection, prov_obs_id)
                    obs_member_uri = ObservationURI(obs_member_uri_str)
                    plane_uri = PlaneURI.get_plane_uri(
                        obs_member_uri, prov_prod_id)
                    plane_inputs.add(plane_uri)
                    logging.debug('Adding PlaneURI {}'.format(plane_uri))

    mc.update_typed_set(plane.provenance.inputs, plane_inputs)


def update_plane_provenance_single(plane, headers, lookup, collection, repair,
                                   obs_id):
    """Add inputs to Planes, based on a particular keyword prefix. This
    differs from update_plane_provenance because all the values are in a
    single keyword, such as COMMENT or HISTORY.

    :param plane Plane instance to add inputs to
    :param headers FITS keyword headers that have lookup values.
    :param lookup The keyword pattern to find in the FITS header keywords for
        input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    """
    plane_inputs = TypedSet(PlaneURI,)

    for header in headers:
        for keyword in header:
            if keyword.startswith(lookup):
                value = header.get(keyword)
                prov_ids = repair(value, obs_id)
                for entry in prov_ids:
                    # 0 - observation
                    # 1 - plane
                    obs_member_uri_str = \
                        mc.CaomName.make_obs_uri_from_obs_id(
                            collection, entry[0])
                    obs_member_uri = ObservationURI(obs_member_uri_str)
                    plane_uri = PlaneURI.get_plane_uri(
                        obs_member_uri, entry[1])
                    plane_inputs.add(plane_uri)
                    logging.debug('Adding PlaneURI {}'.format(plane_uri))
                # because all the content gets processed with one
                # access to the keyword value, stop after one round
                break

    mc.update_typed_set(plane.provenance.inputs, plane_inputs)


def update_observation_members(observation):
    """Add members to Observation from all its Planes.

    :param observation Observation instance to add members to
    """
    members_inputs = TypedSet(ObservationURI,)
    for plane in observation.planes.values():
        if (plane.provenance is not None and
                plane.provenance.inputs is not None):
            for inpt in plane.provenance.inputs:
                members_inputs.add(inpt.get_observation_uri())
                logging.debug('Adding Observation URI {}'.format(
                    inpt.get_observation_uri()))
    mc.update_typed_set(observation.members, members_inputs)


def reset_energy(chunk):
    """
    :param chunk: Set the energy component of a chunk to None as a side-effect.
    """
    chunk.energy = None
    chunk.energy_axis = None


def reset_position(chunk):
    """
    :param chunk: Set the position component of a chunk to None as a
    side-effect.
    """
    chunk.position = None
    chunk.position_axis_1 = None
    chunk.position_axis_2 = None


def reset_observable(chunk):
    """
    :param chunk: Set the observable component of a chunk to None as a
    side-effect.
    """
    chunk.observable = None
    chunk.observable_axis = None

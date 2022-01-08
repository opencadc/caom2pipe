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

from caom2 import CoordAxis1D, Axis, RefCoord, CoordRange1D, SpectralWCS
from caom2 import TypedSet, ObservationURI, PlaneURI, Chunk, CoordPolygon2D
from caom2 import ValueCoord2D, Algorithm, Artifact, Part, TemporalWCS
from caom2 import Instrument, TypedOrderedDict, SimpleObservation, CoordError
from caom2 import CoordFunction1D, DerivedObservation, Provenance
from caom2 import CoordBounds1D, TypedList, ProductType
from caom2.diff import get_differences
from caom2utils import ObsBlueprint, GenericParser, FitsParser
from caom2utils import update_artifact_meta

from caom2pipe import astro_composable as ac
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc

__all__ = [
    'append_plane_provenance',
    'append_plane_provenance_single',
    'build_artifact_uri',
    'build_chunk_energy_range',
    'build_chunk_time',
    'build_temporal_wcs_append_sample',
    'build_temporal_wcs_bounds',
    'change_to_simple',
    'change_to_composite',
    'compare',
    'copy_artifact',
    'copy_chunk',
    'copy_instrument',
    'copy_part',
    'copy_provenance',
    'do_something_to_chunks',
    'exec_footprintfinder',
    'find_plane_and_artifact',
    'Fits2caom2Visitor',
    'get_obs_id_from_cadc',
    'is_composite',
    'make_plane_uri',
    'rename_parts',
    'reset_energy',
    'reset_observable',
    'reset_position',
    'TelescopeMapping',
    'undo_astropy_cdfix_call',
    'update_observation_members',
    'update_observation_members_filtered',
    'update_plane_provenance',
    'update_plane_provenance_from_values',
    'update_plane_provenance_list',
    'update_plane_provenance_single',
]


def append_plane_provenance(
    plane, headers, lookup, collection, repair, obs_id
):
    """Append inputs to Planes, based on a particular keyword prefix.
    This function is NOT for removing inputs that have been previously added.

    :param plane Plane instance to add inputs to
    :param headers FITS keyword headers that have lookup values.
    :param lookup The keyword pattern to find in the FITS header keywords for
        input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    """
    plane_inputs = TypedSet(
        PlaneURI,
    )
    _update_plane_provenance(
        headers, lookup, collection, repair, obs_id, plane_inputs
    )
    plane.provenance.inputs.update(plane_inputs)


def append_plane_provenance_single(
    plane, headers, lookup, collection, repair, obs_id
):
    """Append inputs to Planes, based on a particular keyword prefix. This
    differs from update_plane_provenance because all the values are in a
    single keyword, such as COMMENT or HISTORY. It differs from
    update_plane_provenance_single in that it does NOT replace existing
    inputs with the list that is created here.

    :param plane Plane instance to add inputs to
    :param headers FITS keyword headers that have lookup values.
    :param lookup The keyword pattern to find in the FITS header keywords for
        input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    """
    plane_inputs = TypedSet(
        PlaneURI,
    )
    _find_plane_provenance_single(
        plane_inputs, headers, lookup, collection, repair, obs_id
    )
    plane.provenance.inputs.update(plane_inputs)


def _find_plane_provenance_single(
    plane_inputs, headers, lookup, collection, repair, obs_id
):
    """
    :param plane_inputs TypedSet instance to add inputs to
    :param headers FITS keyword headers that have lookup values.
    :param lookup The keyword pattern to find in the FITS header keywords for
        input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    """
    for header in headers:
        for keyword in header:
            if keyword.startswith(lookup):
                value = header.get(keyword)
                prov_ids = repair(value, obs_id)
                for entry in prov_ids:
                    # 0 - observation
                    # 1 - plane
                    obs_member_uri_str = mc.CaomName.make_obs_uri_from_obs_id(
                        collection, entry[0]
                    )
                    obs_member_uri = ObservationURI(obs_member_uri_str)
                    plane_uri = PlaneURI.get_plane_uri(
                        obs_member_uri, entry[1]
                    )
                    plane_inputs.add(plane_uri)
                    logging.debug(f'Adding PlaneURI {plane_uri}')
                # because all the content gets processed with one
                # access to the keyword value, stop after one round
                break


def build_artifact_uri(file_name, collection, scheme='ad'):
    """
    Common code to create the string that represents a URI for lookup by
    CADC services.
    :return: str representation of a URI
    """
    return f'{scheme}:{collection}/{file_name}'


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
        axis = CoordAxis1D(axis=Axis(ctype='WAVE', cunit='Angstrom'))
        ref_coord1 = RefCoord(0.5, cw - fwhm / 2.0)
        ref_coord2 = RefCoord(1.5, cw + fwhm / 2.0)
        axis.range = CoordRange1D(ref_coord1, ref_coord2)

        energy = SpectralWCS(
            axis=axis,
            specsys='TOPOCENT',
            ssyssrc=None,
            ssysobs=None,
            bandpass_name=filter_name,
            resolving_power=resolving_power,
        )
        chunk.energy = energy
        # PD - in general, do not set the energy_axis, unless the energy axis
        # was really in the fits header


def build_chunk_time(chunk, header, name):
    """

    :param chunk: CAOM2 Chunk instance for which to set time.
    :param header: FITS header with the keywords for value extraction.
    :param name: str  for logging information only.
    :return:
    """
    logging.debug(f'Begin build_chunk_time for {name}.')
    # DB 02-07-20
    # time metadata comes from MJD_OBS and EXPTIME, it's not
    # an axis requiring cutout support
    exp_time = header.get('EXPTIME')
    mjd_obs = header.get('MJD-OBS')
    if exp_time is None or mjd_obs is None:
        chunk.time = None
    else:
        if chunk.time is None:
            coord_error = CoordError(syser=1e-07, rnder=1e-07)
            time_axis = CoordAxis1D(axis=Axis('TIME', 'd'), error=coord_error)
            chunk.time = TemporalWCS(axis=time_axis, timesys='UTC')
        ref_coord = RefCoord(pix=0.5, val=mjd_obs)
        chunk.time.axis.function = CoordFunction1D(
            naxis=1,
            delta=mc.convert_to_days(exp_time),
            ref_coord=ref_coord,
        )
        chunk.time.exposure = exp_time
        chunk.time.resolution = mc.convert_to_days(exp_time)
    logging.debug(f'End build_chunk_time.')


def do_something_to_chunks(observation, do_something, headers, file_uri):
    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            if artifact.uri == file_uri:
                for part in artifact.parts.values():
                    for chunk in part.chunks:
                        do_something(chunk, headers)


def find_keywords_in_headers(headers, lookups):
    """
    Common code to find all the values for a list of keywords with a common
    prefix in a FITS header.
    """
    values = []
    for header in headers:
        values += find_keywords_in_header(header, lookups)
    return values


def find_keywords_in_header(header, lookups):
    """
    Common code to find all the values for a list of keywords with a common
    prefix in a FITS header.
    """
    values = []
    for keyword in header:
        for lookup in lookups:
            if keyword.startswith(lookup):
                values.append(header.get(keyword))
    return values


def build_temporal_wcs_append_sample(temporal_wcs, lower, upper):
    """All the CAOM entities for building a TemporalWCS instance with
    a bounds definition, or appending a sample, in one function.
    """
    if temporal_wcs is None:
        samples = TypedList(
            CoordRange1D,
        )
        bounds = CoordBounds1D(samples=samples)
        temporal_wcs = TemporalWCS(
            axis=CoordAxis1D(
                axis=Axis('TIME', 'd'),
                bounds=bounds,
            ),
            timesys='UTC',
        )
    start_ref_coord = RefCoord(pix=0.5, val=lower)
    end_ref_coord = RefCoord(pix=1.5, val=upper)
    sample = CoordRange1D(start_ref_coord, end_ref_coord)
    temporal_wcs.axis.bounds.samples.append(sample)
    return temporal_wcs


def build_temporal_wcs_bounds(tap_client, plane, collection):
    """Assemble a bounds/sample time definition, based on the inputs
    identified in the header.

    :param tap_client CadcTapClient for querying existing CAOM records for
        time metadata
    :param plane CAOM Plane instance, contains input metadata in the
        provenance.inputs list, and chunks will be updated with the resulting
        temporal WCS information
    :param collection str to scope the query
    """
    logging.debug(f'Begin build_temporal_wcs_bounds.')
    product_ids = []
    for ip in plane.provenance.inputs:
        product_ids.append(ip.get_product_id())
    logging.info(f'Finding temporal inputs for {len(product_ids)} inputs.')

    inputs = []
    # this query makes the assumption that there's a remarkable resemblance
    # between product_id values, and file names, minus all the extensions
    for product_id in product_ids:
        query_string = f"""
        SELECT C.time_axis_function_refCoord_val AS val,
               C.time_axis_function_delta AS delta,
               C.time_axis_axis_cunit AS cunit,
               C.time_axis_function_naxis AS naxis
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P on P.obsID = O.obsID
        JOIN caom2.Artifact AS A on P.planeID = A.planeID
        JOIN caom2.Part AS PT on A.artifactID = PT.artifactID
        JOIN caom2.Chunk AS C on PT.partID = C.partID
        WHERE A.uri like '%{product_id}%'
        AND O.collection = '{collection}'
        """

        table_result = clc.query_tap_client(query_string, tap_client)
        if len(table_result) > 0:
            for row in table_result:
                if row['cunit'] == 'd' and row['naxis'] == 1:
                    inputs.append([row['val'], row['delta']])
                else:
                    logging.warning(
                        f'Could not make use of values for '
                        f'{product_id}.NAXISi is {row["naxis"]} and CUNITi '
                        f'is {row["cunit"]}'
                    )
        else:
            logging.warning(f'No CAOM record for {product_id} found at CADC.')
    logging.info(f'Building temporal bounds for {len(inputs)} inputs.')

    temporal_wcs = None
    for ip in inputs:
        temporal_wcs = build_temporal_wcs_append_sample(
            temporal_wcs, lower=ip[0], upper=(ip[0] + ip[1])
        )

    for artifact in plane.artifacts.values():
        for part in artifact.parts.values():
            if part.product_type == ProductType.SCIENCE:
                for chunk in part.chunks:
                    logging.debug(
                        f'Adding TemporalWCS to chunks in artifact '
                        f'{artifact.uri}, part {part.name}.'
                    )
                    chunk.time = temporal_wcs
    logging.debug(f'End build_temporal_wcs_bounds.')


def change_to_composite(
    observation, algorithm_name='composite'
):
    """For the case where a SimpleObservation needs to become a
    DerivedObservation."""
    temp = DerivedObservation(
        observation.collection,
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
        observation.target_position,
    )
    temp.meta_producer = observation.meta_producer
    temp.last_modified = datetime.utcnow()
    temp._id = observation._id
    return temp


def change_to_simple(observation):
    """For the case where a DerivedObservation needs to become a
    SimpleObservation."""
    temp = SimpleObservation(
        observation.collection,
        observation.observation_id,
        Algorithm('exposure'),
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
        observation.target_position,
    )
    temp.last_modified = datetime.utcnow()
    temp._id = observation._id
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
        msg = (
            f'Differences found obs id {ex.observation_id} file id {entry} '
            f'instr {ex.instrument.name}\n{result_str}'
        )
        return msg
    return None


def copy_artifact(from_artifact, features=None):
    """Make a copy of an artifact. This works around the CAOM2 constraint
    'org.postgresql.util.PSQLException: ERROR: duplicate key value violates
    unique constraint "artifact_pkey"', when trying to use the same artifact
    information in different planes (e.g. when referring to the same
    thumbnail and preview files).

    :param from_artifact Artifact of which to make a shallow copy
    :param features which version of CAOM to use
    :return a copy of the from_artifact, with parts set to None
    """
    if features is not None and features.supports_latest_caom:
        copy = Artifact(
            uri=from_artifact.uri,
            product_type=from_artifact.product_type,
            release_type=from_artifact.release_type,
            content_type=from_artifact.content_type,
            content_length=from_artifact.content_length,
            content_checksum=from_artifact.content_checksum,
            content_release=from_artifact.content_release,
            content_read_groups=from_artifact.content_read_groups,
            parts=None,
        )
    else:
        copy = Artifact(
            uri=from_artifact.uri,
            product_type=from_artifact.product_type,
            release_type=from_artifact.release_type,
            content_type=from_artifact.content_type,
            content_length=from_artifact.content_length,
            content_checksum=from_artifact.content_checksum,
            parts=None,
        )
    return copy


def copy_chunk(from_chunk, features=None):
    """Make a copy of a Chunk. This works around the CAOM2 constraint
    'org.postgresql.util.PSQLException: ERROR: duplicate key value violates
    unique constraint "chunk_pkey"', when trying to use the same chunk
    information in different parts (e.g. when referring to hdf5 files).

    :param from_chunk Chunk of which to make a copy
    :param features which version of CAOM to use
    :return a copy of the from_chunk
    """
    if features is not None and features.supports_latest_caom:
        copy = Chunk(
            product_type=from_chunk.product_type,
            naxis=from_chunk.naxis,
            position_axis_1=from_chunk.position_axis_1,
            position_axis_2=from_chunk.position_axis_2,
            position=from_chunk.position,
            energy_axis=from_chunk.energy_axis,
            energy=from_chunk.energy,
            time_axis=from_chunk.time_axis,
            time=from_chunk.time,
            custom_axis=from_chunk.custom_axis,
            custom=from_chunk.custom,
            polarization_axis=from_chunk.polarization_axis,
            polarization=from_chunk.polarization,
            observable_axis=from_chunk.observable_axis,
            observable=from_chunk.observable,
        )
        copy.meta_producer = from_chunk.meta_producer
    else:
        copy = Chunk(
            product_type=from_chunk.product_type,
            naxis=from_chunk.naxis,
            position_axis_1=from_chunk.position_axis_1,
            position_axis_2=from_chunk.position_axis_2,
            position=from_chunk.position,
            energy_axis=from_chunk.energy_axis,
            energy=from_chunk.energy,
            time_axis=from_chunk.time_axis,
            time=from_chunk.time,
            polarization_axis=from_chunk.polarization_axis,
            polarization=from_chunk.polarization,
            observable_axis=from_chunk.observable_axis,
            observable=from_chunk.observable,
        )
    return copy


def copy_instrument(from_instrument, new_name):
    """Make a copy of an Instrument. This is the only way to change an
    Instrument name.

    :param from_instrument Instrument of which to make a copy
    :param new_name String for Instrument.name parameter
    :return a copy of the from_instrument
    """
    copy = Instrument(name=new_name)
    for entry in from_instrument.keywords:
        copy.keywords.add(entry)
    return copy


def copy_part(from_part, features=None):
    """Make a copy of a Part. This works around the CAOM2 constraint
    'org.postgresql.util.PSQLException: ERROR: duplicate key value violates
    unique constraint "part_pkey"', when trying to use the same part
    information in different artifacts (e.g. when referring to hdf5 files).

    :param from_part Part of which to make a shallow copy
    :param features which version of CAOM to use
    :return a copy of the from_part, with chunks set to None
    """
    copy = Part(
        name=from_part.name,
        product_type=from_part.product_type,
        chunks=None,
    )
    if features is not None and features.supports_latest_caom:
        copy.meta_producer = from_part.meta_producer
    return copy


def copy_provenance(from_provenance):
    """Make a deep copy of a Provenance instance.
    :param from_provenance Provenance of which to make a shallow copy
    :return a copy of the from_provenance, with keywords set to None
    """
    copy = Provenance(
        name=from_provenance.name,
        version=from_provenance.version,
        project=from_provenance.project,
        producer=from_provenance.producer,
        run_id=from_provenance.run_id,
        reference=from_provenance.reference,
        last_executed=from_provenance.last_executed,
    )
    for entry in from_provenance.inputs:
        copy.inputs.add(entry)
    for entry in from_provenance.keywords:
        copy.keywords.add(entry)
    return copy


def exec_footprintfinder(
    chunk, science_fqn, log_file_directory, obs_id, params='-f'
):
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
    logging.debug(f'Begin _update_position for {obs_id}')
    mc.check_param(chunk, Chunk)

    # local import because footprintfinder depends on matplotlib being
    # installed, which is not declared as a caom2pipe dependency
    import footprintfinder

    if chunk.position is not None and chunk.position.axis is not None:
        logging.debug(
            f'position exists, calculate footprints for {science_fqn}.'
        )
        for parameters in [params, f'{params} -m 0.2', '-f']:
            # try in decreasing fidelity to get a Polygon that is supported
            # by CAOM's Polygon/MultiPolygon structures
            #
            # -m 0.2 fewer points
            # -f full chip

            (
                full_area,
                footprint_xc,
                footprint_yc,
                ra_bary,
                dec_bary,
                footprintstring,
                stc,
            ) = footprintfinder.main(f'-r {parameters} {science_fqn}')
            logging.debug(
                f'footprintfinder result: full area {full_area} footprint xc '
                f'{footprint_xc} footprint yc {footprint_yc} ra bary '
                f'{ra_bary} dec_bary {dec_bary} footprintstring '
                f'{footprintstring} stc {stc}'
            )
            coords = None
            fp_results = stc.split('Polygon FK5')
            if len(fp_results) > 1:
                coords = fp_results[1].split()
            else:
                fp_results = stc.split('Polygon ICRS')
                if len(fp_results) > 1:
                    coords = fp_results[1].split()

            if coords is not None:
                break

        if coords is None:
            raise mc.CadcException(F'Do not recognize footprint {stc}')

        bounds = CoordPolygon2D()
        index = 0
        while index < len(coords):
            vertex = ValueCoord2D(
                mc.to_float(coords[index]),
                mc.to_float(coords[index + 1]),
            )
            bounds.vertices.append(vertex)
            index += 2
            logging.debug(f'Adding vertex\n{vertex}')
        chunk.position.axis.bounds = bounds

        prefix = os.path.basename(science_fqn).replace('.fits', '')
        return_file = f'{prefix}_footprint.txt'
        return_string_file = f'{prefix}_footprint_returnstring.txt'
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
            logging.debug(
                f'Moving footprint log file from {orig_log_fqn} to {log_fqn}'
            )
    else:
        logging.debug(f'Removing footprint log file {orig_log_fqn}')
        os.unlink(orig_log_fqn)


def find_plane_and_artifact(observation, product_id, uri):
    """
    Find the particular plane/artifact combination referenced by a product id
    and URI.

    :param observation: Observation in which to find the plane/artifact
        combination
    :param product_id: Plane lookup key
    :param uri: Artifact lookup key
    :return: Plane, Artifact instance that are referenced by the in-coming
        parameters.
    """
    plane = None
    artifact = None
    if product_id in observation.planes.keys():
        plane = observation.planes[product_id]
        if uri in plane.artifacts.keys():
            artifact = plane.artifacts[uri]
        else:
            # return all assigned a value, or all None
            plane = None
    return plane, artifact


def get_all_artifact_keys(observation):
    """
    :param observation: Observation
    :return: a list of all Artifact keys in the Observation
    """
    all_artifact_keys = []
    for plane in observation.planes.values():
        for key in plane.artifacts.keys():
            all_artifact_keys.append(key)
    return all_artifact_keys


def get_obs_id_from_cadc(artifact_uri, tap_client):
    """
    Query CAOM using TAP for the observation ID, given a file ID.

    :param artifact_uri: URI a Artifact URI that may or may not
        exist in CAOM
    :param tap_client: CadcTapClient - used to execute the query
    :return: string representing an observation ID, or None, if no
        entry is found.
    """
    logging.debug(f'Begin get_obs_id_from_cadc for {artifact_uri}')
    query_string = f"""
    SELECT DISTINCT O.observationID 
    FROM caom2.Observation AS O
    JOIN caom2.Plane AS P on P.obsID = O.obsID
    JOIN caom2.Artifact AS A on A.planeID = P.planeID
    WHERE A.uri = '{artifact_uri}'
    """
    table = clc.query_tap_client(query_string, tap_client)
    result = None
    if len(table) >= 1:
        result = table[0]['observationID']
    logging.debug(f'End get_obs_id_from_cadc {result}')
    return result


def is_composite(headers, keyword_prefix='IMCMB'):
    """All the logic to determine if a file name is part of a
    CompositeObservation, in one marvelous function."""
    result = False

    if len(headers) > 0:
        # look in the last header - IMCMB keywords are not in the zero'th
        # header
        header = headers[-1]
        for ii in header:
            if ii.startswith(keyword_prefix):
                result = True
                break
    return result


def make_plane_uri(obs_id, product_id, collection):
    """
    Common code to construction a PlaneURI.

    :param obs_id: str Observation.observationID for a CADC collection.
    :param product_id: str Plane.productID for a CADC collection.
    :param collection: str CADC collection.
    :return: tuple with ObservationURI, PlaneURI instance
    """
    obs_member_uri_str = mc.CaomName.make_obs_uri_from_obs_id(
        collection, obs_id
    )
    obs_member_uri = ObservationURI(obs_member_uri_str)
    plane_uri = PlaneURI.get_plane_uri(obs_member_uri, product_id)
    return obs_member_uri, plane_uri


def rename_parts(observation, headers):
    """
    By default, the values for part.name are extension numbers. Replace those
    with the value of the EXTNAME keyword. The part.name is the key value in
    the TypedOrderedDict, so this is done as a pop/push.

    :param observation Observation instance with parts that may have the
        wrong names
    :param headers astropy FITS Header list
    """
    part_keys = [str(ii) for ii in range(1, headers[0].get('NEXTEND') + 1)]
    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            temp_parts = TypedOrderedDict(
                Part,
            )
            for part_key in part_keys:
                if part_key in artifact.parts:
                    hdu_count = mc.to_int(part_key)
                    temp = artifact.parts.pop(part_key)
                    temp.name = headers[hdu_count].get('EXTNAME')
                    temp_parts.add(temp)
            for part in temp_parts.values():
                artifact.parts.add(part)


def _update_plane_provenance(
    headers, lookup, collection, repair, obs_id, plane_inputs
):
    """Add inputs to a collection, based on a particular keyword prefix.

    :param headers FITS keyword headers that have lookup values.
    :param lookup The keyword pattern to find in the FITS header keywords for
        input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    :param plane_inputs TypedSet(PlaneURI,) to which new PlaneURI instances are
        added
    """
    for header in headers:
        for keyword in header:
            if keyword.startswith(lookup):
                value = header.get(keyword)
                prov_obs_id, prov_prod_id = repair(value, obs_id)
                if prov_obs_id is not None and prov_prod_id is not None:
                    obs_member_uri_str = \
                        mc.CaomName.make_obs_uri_from_obs_id(
                            collection, prov_obs_id
                        )
                    obs_member_uri = ObservationURI(obs_member_uri_str)
                    plane_uri = PlaneURI.get_plane_uri(
                        obs_member_uri, prov_prod_id
                    )
                    plane_inputs.add(plane_uri)
                    logging.debug(f'Adding PlaneURI {plane_uri}')


def update_plane_provenance(
    plane, headers, lookup, collection, repair, obs_id
):
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
    plane_inputs = TypedSet(
        PlaneURI,
    )
    _update_plane_provenance(
        headers, lookup, collection, repair, obs_id, plane_inputs
    )
    mc.update_typed_set(plane.provenance.inputs, plane_inputs)


def update_plane_provenance_from_values(
    plane, repair, values, collection, obs_id
):
    """
    Add inputs to Planes, based on a list of input values.

    :param plane: Plane instance to add inputs to
    :param repair: The function to fix the input values, to ensure they
        match input observationID values
    :param values: list of values to add as inputs, after repair
    :param collection: str The collection name for URI construction
    :param obs_id: str value for logging only
    :return:
    """
    logging.debug(f'Begin update_plane_provenance_from_values')
    plane_inputs = TypedSet(PlaneURI,)
    for value in values:
        prov_obs_id, prov_prod_id = repair(value, obs_id)
        if prov_obs_id is not None and prov_prod_id is not None:
            obs_member_uri_ignore, plane_uri = make_plane_uri(
                prov_obs_id, prov_prod_id, collection
            )
            plane_inputs.add(plane_uri)
            logging.debug(f'Adding PlaneURI {plane_uri}')
    mc.update_typed_set(plane.provenance.inputs, plane_inputs)
    logging.debug(f'End update_plane_provenance_from_values')


def update_plane_provenance_list(
    plane, headers, lookups, collection, repair, obs_id
):
    """Add inputs to Planes, based on a particular keyword prefix.

    :param plane Plane instance to add inputs to
    :param headers FITS keyword headers that have lookup values.
    :param lookups a list of keyword patterns to find in the FITS header
        keywords for input files.
    :param collection The collection name for URI construction
    :param repair The function to fix input values, to ensure they match
        input observation ID values.
    :param obs_id String value for logging only.
    """
    plane_inputs = TypedSet(
        PlaneURI,
    )

    for entry in lookups:
        _update_plane_provenance(
            headers, entry, collection, repair, obs_id, plane_inputs
        )
    mc.update_typed_set(plane.provenance.inputs, plane_inputs)


def update_plane_provenance_single(
    plane, headers, lookup, collection, repair, obs_id
):
    """Replace inputs in Planes, based on a particular keyword prefix. This
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
    plane_inputs = TypedSet(
        PlaneURI,
    )
    _find_plane_provenance_single(
        plane_inputs, headers, lookup, collection, repair, obs_id
    )
    mc.update_typed_set(plane.provenance.inputs, plane_inputs)


def update_observation_members(observation):
    """Add members to Observation from all its Planes.

    :param observation Observation instance to add members to
    """
    members_inputs = TypedSet(
        ObservationURI,
    )
    for plane in observation.planes.values():
        if (
            plane.provenance is not None
            and plane.provenance.inputs is not None
        ):
            for inpt in plane.provenance.inputs:
                members_inputs.add(inpt.get_observation_uri())
                logging.debug(
                    f'Adding Observation URI {inpt.get_observation_uri()}'
                )
    mc.update_typed_set(observation.members, members_inputs)


def update_observation_members_filtered(observation, filter_fn):
    """Add members to Observation from all its Planes, depending
    on the return of the filter_fn.

    :param observation Observation instance to add members to
    :param filter_fn returns True if a plane input should also be an
        Observation member
    """

    inputs = []
    members_inputs = TypedSet(
        ObservationURI,
    )
    for plane in observation.planes.values():
        if (
            plane.provenance is not None
            and plane.provenance.inputs is not None
        ):
            inputs = filter(filter_fn, plane.provenance.inputs)

    for entry in inputs:
        members_inputs.add(entry.get_observation_uri())
        logging.debug(f'Adding Observation URI {entry.get_observation_uri()}')
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


def undo_astropy_cdfix_call(chunk, time_delta):
    """
    undo the effects of the astropy cdfix call on a
    matrix, in fits2caom2.WcsParser, which sets the
    diagonal element of the matrix to unity if all
    keywords associated with a given axis were
    omitted.
    See:
    https://docs.astropy.org/en/stable/api/astropy.
    wc.Wcsprm.html#astropy.wcs.Wcsprm.cdfix
    """
    if (
        time_delta == 0.0
        and chunk.time is not None
        and chunk.time.axis is not None
        and chunk.time.axis.function is not None
        and chunk.time.axis.function.delta == 1.0
    ):
        chunk.time.axis.function.delta = 0.0


class TelescopeMapping:
    """
    A default implementation for building up and applying an ObsBlueprint
    map for a file, and then doing any n:n (FITS keywords:CAOM2 keywords)
    mapping, using the 'update' method.
    """

    def __init__(self, storage_name, headers):
        self._storage_name = storage_name
        self._headers = headers
        self._logger = logging.getLogger(self.__class__.__name__)

    def accumulate_blueprint(self, bp, application=None):
        """
        Configure the telescope-specific ObsBlueprint at the CAOM model
        Observation level.
        """
        self._logger.debug(
            f'Begin accumulate_blueprint for {self._storage_name.file_uri}'
        )
        if application is not None:
            meta_producer = mc.get_version(application)
            bp.set('Observation.metaProducer', meta_producer)
            bp.set('Plane.metaProducer', meta_producer)
            bp.set('Artifact.metaProducer', meta_producer)
            bp.set('Chunk.metaProducer', meta_producer)

    def _update_artifact(self, artifact, caom_repo_client=None):
        return

    def update(self, observation, file_info, caom_repo_client=None):
        self._logger.debug(f'Begin update for {observation.observation_id}')
        for plane in observation.planes.values():
            for artifact in plane.artifacts.values():
                if artifact.uri != self._storage_name.file_uri:
                    self._logger.debug(
                        f'Artifact uri is {artifact.uri} but working on '
                        f'{self._storage_name.file_uri}. Continuing.'
                    )
                    continue
                update_artifact_meta(artifact, file_info)
                self._update_artifact(artifact, caom_repo_client)

        self._logger.debug('End update')
        return observation


class Fits2caom2Visitor:
    """
    Use a TelescopeMapping specialization instance to create a CAOM2
    record, as expected by the execute_composable.MetaVisits class.
    """

    def __init__(self, observation, **kwargs):
        self._observation = observation
        self._storage_name = kwargs.get('storage_name')
        self._metadata_reader = kwargs.get('metadata_reader')
        self._caom_repo_client = kwargs.get('caom_repo_client')
        self._dump_config = False
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_mapping(self, headers):
        return TelescopeMapping(self._storage_name, headers)

    def visit(self):
        self._logger.debug('Begin visit')
        for uri, file_info in self._metadata_reader.file_info.items():
            headers = self._metadata_reader.headers.get(uri)
            telescope_data = self._get_mapping(headers)
            blueprint = ObsBlueprint(instantiated_class=telescope_data)
            telescope_data.accumulate_blueprint(blueprint)

            if len(headers) == 0:
                self._logger.debug(
                    f'No headers, using a GenericParser for '
                    f'{self._storage_name.file_uri}'
                )
                parser = GenericParser(blueprint, uri)
            else:
                self._logger.debug(
                    f'Using a FitsParser for {self._storage_name.file_uri}'
                )
                parser = FitsParser(headers, blueprint, uri)

            if self._dump_config:
                print(f'Blueprint for {uri}: {blueprint}')

            if self._observation is None:
                if blueprint._get('DerivedObservation.members') is None:
                    self._logger.debug('Build a SimpleObservation')
                    self._observation = SimpleObservation(
                        collection=self._storage_name.collection,
                        observation_id=self._storage_name.obs_id,
                        algorithm=Algorithm('exposure'),
                    )
                else:
                    self._logger.debug('Build a DerivedObservation')
                    self._observation = DerivedObservation(
                        collection=self._storage_name.collection,
                        observation_id=self._storage_name.obs_id,
                        algorithm=Algorithm('composite'),
                    )

            parser.augment_observation(
                observation=self._observation,
                artifact_uri=uri,
                product_id=self._storage_name.product_id,
            )

            self._observation = telescope_data.update(
                self._observation,
                file_info,
                self._caom_repo_client,
            )
        self._logger.debug(f'End visit')
        return self._observation

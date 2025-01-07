# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

from caom2 import Observation
from caom2pipe.manage_composable import build_uri, CadcException, check_param


__all__ = ['ArtifactCleanupVisitor']


class ArtifactCleanupVisitor:
    """
    Common code for removing artifacts from an Observation. Over-ride
    'check_for_delete' method, if the characteristics of the deletion change.
    """

    def __init__(self, archive, scheme='ad'):
        self._archive = archive
        self._scheme = scheme
        self._logger = logging.getLogger(self.__class__.__name__)

    def visit(self, observation, **kwargs):
        check_param(observation, Observation)
        plane_count = 0
        artifact_count = 0
        plane_temp = []
        for plane in observation.planes.values():
            artifact_temp = []
            for artifact in plane.artifacts.values():
                if self.check_for_delete(artifact.uri, **kwargs):
                    artifact_temp.append(artifact.uri)

            artifact_delete_list = list(set(artifact_temp))
            for entry in artifact_delete_list:
                self._logger.warning(
                    f'Removing artifact {entry} from observation '
                    f'{observation.observation_id}, plane {plane.product_id}.'
                )
                artifact_count += 1
                observation.planes[plane.product_id].artifacts.pop(entry)

            if len(plane.artifacts) == 0:
                plane_temp.append(plane.product_id)

        plane_delete_list = list(set(plane_temp))
        for entry in plane_delete_list:
            self._logger.warning(f'Removing plane {entry} from observation ' f'{observation.observation_id}.')
            plane_count += 1
            observation.planes.pop(entry)

        self._logger.info(
            f'Completed artifact cleanup augmentation for '
            f'{observation.observation_id}. Removed {artifact_count} '
            f'artifacts, {plane_count} planes from the observation.'
        )
        return {
            'artifacts': artifact_count,
            'planes': plane_count,
        }

    def check_for_delete(self, uri, **kwargs):
        """
        Return True if an artifact should be deleted from an Observation in
        a collection.

        :param uri:
        :param kwargs: Assume the 'url' entry in kwargs is actually a file
            name for the default implementation.
        :return:
        """
        url = kwargs.get('url')
        if url is None:
            raise CadcException('Must have a "url" parameter for ArtifactCleanupVisitor.')
        candidate_uri = build_uri(
            scheme=self._scheme,
            archive=self._archive,
            file_name=url,
        )
        return candidate_uri == uri

# -*- coding: utf-8 -*-
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
import os

from caom2pipe import manage_composable as mc


__all__ = ['CadcTransfer', 'Transfer', 'VoTransfer']


class Transfer(object):
    """
    No-op class to represent the actions of transferring a file from
    one point to another.
    """

    def __init__(self):
        self._client = None

    def get(self, source, dest_fqn):
        """
        This no-op implementation is used when use_local_files = True.
        :param source:
        :param dest_fqn: str - file-system based fully-qualified name
        """
        pass

    def check(self, dest_fqn):
        """
        :param dest_fqn: str - file-system based fully-qualified name
        """
        return True


class CadcTransfer(Transfer):
    """
    Uses the CadcDataClient to manage transfers from CADC to local disk.
    """

    def __init__(self, config):
        super(CadcTransfer, self).__init__()
        from cadcdata import CadcDataClient
        subject = mc.define_subject(config)
        self._client = CadcDataClient(subject)
        self._metrics = None
        self._logger = logging.getLogger(__name__)

    def get(self, source, dest_fqn):
        """
        :param source: str - artifact uri
        :param dest_fqn: str - fully-qualified file system name
        """
        working_dir = os.path.dirname(dest_fqn)
        f_name = os.path.basename(dest_fqn)
        scheme_ignore, archive, f_name_ignore = mc.decompose_uri(source)
        mc.data_get(self._client, working_dir, f_name, archive, self._metrics)

    def check(self, dest_fqn):
        """Assumes fits files at this time. Returns true because the
        CadcDataClient implementation is configured to already do an
        md5 checksum."""
        return True


class VoTransfer(Transfer):
    """
    Uses the vos Client to manage transfers from CADC to local disk.
    """

    def __init__(self, config):
        import vos
        super(VoTransfer, self).__init__()
        if not os.path.exists(config.proxy_fqn):
            raise mc.CadcException('Require a certificate for vos access.')
        self._client = vos.Client(vospace_certfile=config.proxy_fqn)
        self._metrics = None
        self._logger = logging.getLogger(__name__)

    def get(self, source, dest_fqn):
        self._client.copy(source, dest_fqn, send_md5=True)


class FitsTransfer(Transfer):
    """
    Abstract class to provide FITS transfer checking.
    """

    def __init__(self, observable):
        self._observable = observable
        self._logger = logging.getLogger(__name__)

    def get(self, source, dest_fqn):
        raise NotImplementedError

    def check(self, dest_fqn):
        from astropy.io import fits
        try:
            hdulist = fits.open(dest_fqn, memmap=True, lazy_load_hdus=False)
            hdulist.verify('warn')
            for h in hdulist:
                h.verify('warn')
            hdulist.close()
        except (fits.VerifyError, OSError) as e:
            self._observable.rejected.record(mc.Rejected.BAD_DATA,
                                             os.path.basename(dest_fqn))
            os.unlink(dest_fqn)
            raise mc.CadcException(
                f'astropy verify error {dest_fqn} when reading {e}')
        # a second check that fails for some NEOSSat cases - if this works,
        # the file might have been correctly retrieved
        try:
            # ignore the return value - if the file is corrupted, the getdata
            # fails, which is the only interesting behaviour here
            fits.getdata(dest_fqn, ext=0)
        except TypeError as e:
            self._observable.rejected.record(mc.Rejected.BAD_DATA,
                                             os.path.basename(dest_fqn))
            os.unlink(dest_fqn)
            raise mc.CadcException(
                f'astropy getdata error {dest_fqn} when reading {e}')


class HttpTransfer(FitsTransfer):
    """
    Uses HTTP to manage transfers from external sites to local disk.
    """

    def __init__(self, observable):
        self._observable = observable
        self._logger = logging.getLogger(__name__)

    def get(self, source, dest_fqn):
        """
        :param source: HTTP URL
        :param dest_fqn: fully-qualified string that represents file name
        :return:
        """
        self._logger.debug(f'Retrieve {source}')
        mc.http_get(source, dest_fqn)
        if '.fits' in dest_fqn:
            self.check(dest_fqn)
        self._logger.debug(f'Successfully retrieved {source}')


class FtpTransfer(FitsTransfer):
    """
    Uses FTP to manage transfers from external sites to local disk.
    """

    def __init__(self, ftp_host, observable):
        self._ftp_host = ftp_host
        self._observable = observable
        self._logger = logging.getLogger(__name__)

    def get(self, source, dest_fqn):
        """
        :param source: FTP URL
        :param dest_fqn: fully-qualified string that represents file name
        :return:
        """
        self._logger.debug(f'Retrieve {source}')
        mc.ftp_get_timeout(self._ftp_host, source, dest_fqn)
        if '.fits' in dest_fqn:
            self.check(dest_fqn)
        self._logger.debug(f'Successfully retrieved {source}')

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
import traceback

from caom2pipe import manage_composable as mc


__all__ = [
    'CadcTransfer',
    'FtpTransfer',
    'HttpTransfer',
    'Transfer',
    'VoFitsCleanupTransfer',
    'VoFitsTransfer',
    'VoTransfer',
]


class Transfer(object):
    """
    No-op class to represent the actions of transferring a file from
    one point to another.
    """

    def __init__(self):
        self._client = None
        self._observable = None

    @property
    def observable(self):
        return self._observable

    @observable.setter
    def observable(self, value):
        self._observable = value

    def get(self, source, dest_fqn):
        """
        This no-op implementation is used when use_local_files = True.
        :param source:
        :param dest_fqn: str - file-system based fully-qualified name
        """
        pass

    def check(self, dest_fqn, original_fqn):
        """
        :param dest_fqn: str - file-system based fully-qualified name
        :param original_fqn: str - in case the file originates somewhere
            besides the local file-system.
        """
        return True

    def post_store_check(self, source_fqn, dest_fqn):
        """
        After a file is stored to CADC, check that it has the same md5sum at
        CADC as at its source.
        :param source_fqn: str - a fully-qualified name that will make sense
          to a source client.
        :param dest_fqn: str - a fully-qualified name that will make sense to
          CADC storage.
        """
        return True


class CadcTransfer(Transfer):
    """
    Uses the StorageClientWrapper to manage transfers from CADC to local disk.
    """

    def __init__(self):
        super(CadcTransfer, self).__init__()
        self._cadc_client = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def cadc_client(self):
        return self._cadc_client

    @cadc_client.setter
    def cadc_client(self, value):
        self._cadc_client = value

    def get(self, source, dest_fqn):
        """
        :param source: str - artifact uri
        :param dest_fqn: str - fully-qualified file system name
        """
        self._logger.debug(f'Transfer from {source} to {dest_fqn}.')
        working_dir = os.path.dirname(dest_fqn)
        self._cadc_client.get(working_dir, source)

    def check(self, dest_fqn, original_fqn):
        """Assumes fits files at this time. Returns true because the
        CadcDataClient implementation is configured to already do an
        md5 checksum."""
        return True


class VoTransfer(Transfer):
    """
    Uses the vos Client to manage transfers from CADC to local disk.
    """

    def __init__(self):
        super(VoTransfer, self).__init__()
        self._cadc_client = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def cadc_client(self):
        return self._cadc_client

    @cadc_client.setter
    def cadc_client(self, value):
        self._cadc_client = value

    def get(self, source, dest_fqn):
        self._logger.debug(f'Transfer from {source} to {dest_fqn}.')
        self._cadc_client.copy(source, dest_fqn, send_md5=True)


class FitsTransfer(Transfer):
    """
    Abstract class to provide FITS transfer checking.
    """

    def __init__(self):
        self._observable = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def observable(self):
        return self._observable

    @observable.setter
    def observable(self, value):
        self._observable = value

    def check(self, dest_fqn, original_fqn):
        from astropy.io import fits
        try:
            hdulist = fits.open(dest_fqn, memmap=True, lazy_load_hdus=False)
            hdulist.verify('warn')
            for h in hdulist:
                h.verify('warn')
            hdulist.close()
        except (fits.VerifyError, OSError) as e1:
            if self._observable is not None:
                self._observable.rejected.record(
                    mc.Rejected.BAD_DATA, os.path.basename(dest_fqn)
                )
            msg = f'astropy verify error {dest_fqn} when reading {e1}'
            self.failure_action(original_fqn, dest_fqn, msg)
        # a second check that fails for some NEOSSat cases - if this works,
        # the file might have been correctly retrieved
        try:
            # ignore the return value - if the file is corrupted, the getdata
            # fails, which is the only interesting behaviour here
            fits.getdata(dest_fqn)
        except (TypeError, OSError) as e2:
            if self._observable is not None:
                self._observable.rejected.record(
                    mc.Rejected.BAD_DATA, os.path.basename(dest_fqn)
                )
            msg = f'astropy getdata error {dest_fqn} when reading {e2}'
            self.failure_action(original_fqn, dest_fqn, msg)

    def get(self, source, dest_fqn):
        raise NotImplementedError

    def failure_action(self, original_fqn, destination_fqn, msg):
        """Action take on failure is completely dependent on where the
        file originated, and any cleanup configuration."""
        try:
            if os.path.exists(destination_fqn):
                os.unlink(destination_fqn)
        except Exception as e:
            self._logger.error(
                f'Failed to clean up {destination_fqn} after a verification '
                f'error.'
            )
            raise mc.CadcException(e)
        raise mc.CadcException(msg)


class HttpTransfer(FitsTransfer):
    """
    Uses HTTP to manage transfers from external sites to local disk.
    """

    def __init__(self):
        self._observable = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def get(self, source, dest_fqn):
        """
        :param source: HTTP URL
        :param dest_fqn: fully-qualified string that represents file name
        :return:
        """
        self._logger.debug(f'Transfer from {source} to {dest_fqn}.')
        mc.http_get(source, dest_fqn)
        if '.fits' in dest_fqn:
            self.check(dest_fqn, source)
        self._logger.debug(f'Successfully retrieved {source}')


class FtpTransfer(FitsTransfer):
    """
    Uses FTP to manage transfers from external sites to local disk.
    """

    def __init__(self, ftp_host):
        self._ftp_host = ftp_host
        self._observable = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def get(self, source, dest_fqn):
        """
        :param source: FTP URL
        :param dest_fqn: fully-qualified string that represents file name
        :return:
        """
        self._logger.debug(f'Transfer from {source} to {dest_fqn}.')
        mc.ftp_get_timeout(self._ftp_host, source, dest_fqn)
        if '.fits' in dest_fqn:
            self.check(dest_fqn, source)
        self._logger.debug(f'Successfully retrieved {source}')


class VoFitsTransfer(FitsTransfer):
    """
    Uses the vos Client to manage transfers from CADC to local disk. Have
    FITS integrity-checking.
    """

    def __init__(self, vo_client):
        super(VoFitsTransfer, self).__init__()
        self._vo_client = vo_client
        self._logger = logging.getLogger(self.__class__.__name__)

    def get(self, source, dest_fqn):
        self._logger.debug(f'Transfer from {source} to {dest_fqn}.')
        self._vo_client.copy(source, dest_fqn, send_md5=True)
        if '.fits' in dest_fqn:
            self.check(dest_fqn, source)
        self._logger.debug(f'Successfully retrieved {source}')


class VoFitsCleanupTransfer(VoFitsTransfer):
    """
    Implements the case where clean up needs to occur.
    """

    def __init__(self, vo_client, config):
        super(VoFitsCleanupTransfer, self).__init__(vo_client)
        self._cleanup_when_storing = config.cleanup_files_when_storing
        self._failure_destination = config.cleanup_failure_destination
        self._success_destination = config.cleanup_success_destination

    def get(self, source, dest_fqn):
        self._logger.debug(f'Transfer from {source} to {dest_fqn}.')
        try:
            self._vo_client.copy(source, dest_fqn, send_md5=True)
            if '.fits' in dest_fqn:
                self.check(dest_fqn, source)
            self._logger.debug(f'Successfully retrieved {source}')
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            self._logger.error(
                f'Failed to return {source} to {dest_fqn}  with error {e}.'
            )

    def failure_action(self, original_fqn, destination_fqn, msg):
        try:
            if os.path.exists(destination_fqn):
                os.unlink(destination_fqn)
        except Exception as e:
            self._logger.error(
                f'Failed to clean up {destination_fqn} after a verification '
                f'error.'
            )
            raise mc.CadcException(e)

        self._move_action(original_fqn, self._failure_destination)

    def _move_action(self, original_fqn, destination):
        self._logger.debug('Begin _move_action')
        if self._cleanup_when_storing:
            f_name = os.path.basename(original_fqn)
            move_destination = os.path.join(destination, f_name)
            try:
                self._vo_client.move(original_fqn, move_destination)
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                self._logger.error(
                    f'Failed to move {original_fqn} to {move_destination} '
                    f' with error {e}.'
                )
                raise mc.CadcException(e)
        self._logger.debug('Done _move_action')

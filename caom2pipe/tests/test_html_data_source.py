# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2023.                            (c) 2023.
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

from caom2pipe import html_data_source
from caom2pipe.manage_composable import make_datetime, State
from datetime import datetime
from mock import Mock, patch
from treelib import Tree

import test_conf


def test_trees(test_config):
    test_subject = html_data_source.HtmlFilteredPagesTemplate(test_config)
    test_html_filters = html_data_source.HtmlFilter(
        html_data_source.HtmlFilteredPagesTemplate.always_true_filter, True
    )

    in_test_tree = Tree()
    in_test_tree.create_node(tag='ROOT', identifier='root', data=test_html_filters)
    in_test_tree.create_node(
        tag='https://localhost:8081', identifier='test_node_1', parent='root', data=test_html_filters
    )
    to_test_node = in_test_tree.get_node('test_node_1')

    test_entries = {
        'https://localhost:8081/123': datetime(2023, 4, 18, 10, 10, 10),
        'https://localhost:8081/456': datetime(2023, 4, 18, 11, 11, 11),
    }
    assert in_test_tree.size() == 2, 'precondition'
    test_subject.add_children(to_test_node, in_test_tree, test_entries)
    assert in_test_tree.size() == 4, 'post-condition'
    test_leaves = in_test_tree.leaves()
    assert len(test_leaves) == 2, 'wrong number of leaves'
    first_test_leaf = test_leaves[0]
    assert first_test_leaf.tag == 'https://localhost:8081/123', 'wrong tag name'
    assert first_test_leaf.data.fn.__name__ == 'filter_never', 'always_true_filter wrong filter type assigned'

    more_test_entries = {'https://localhost:8081/123/abc.fits': datetime(2023, 4, 18, 10, 10, 10)}
    test_subject.add_children(first_test_leaf, in_test_tree, more_test_entries)
    assert in_test_tree.size() == 5, 'post-condition'
    test_leaves = in_test_tree.leaves()
    assert len(test_leaves) == 2, 'wrong number of leaves, 2nd time'
    last_test_leaf = test_leaves[-1]
    assert last_test_leaf.tag == 'https://localhost:8081/123/abc.fits', 'wrong tag name for leaf 2'
    assert last_test_leaf.data.fn.__name__ == 'filter_by_extensions', 'file_filter wrong filter type assigned'


@patch('caom2pipe.html_data_source.query_endpoint_session')
def test_http(query_mock, test_config, tmp_path):

    def _close():
        pass

    def _query_mock_side_effect(url, ignore_session):
        top_page = f'{test_conf.TEST_DATA_DIR}/top_page.html'
        year_page = f'{test_conf.TEST_DATA_DIR}/year_page.html'
        day_page = f'{test_conf.TEST_DATA_DIR}/day_page.html'

        result = type('response', (), {})
        result.close = _close
        result.raise_for_status = _close

        if url.endswith('ASTRO/'):
            with open(top_page) as f_in:
                result.text = f_in.read()
        elif (
            url.endswith('2021/')
            or url.endswith('2022/')
            or url.endswith('2017/')
            or url.endswith('2018/')
            or url.endswith('2019/')
            or url.endswith('2020/')
            or url.endswith('NESS/')
        ):
            with open(year_page) as f_in:
                result.text = f_in.read()
        elif (
            url.endswith('001/')
            or url.endswith('002/')
            or url.endswith('310/')
            or url.endswith('311/')
            or url.endswith('312/')
            or url.endswith('313/')
        ):
            with open(day_page) as f_in:
                result.text = f_in.read()
        else:
            raise Exception(f'wut {url}')

        return result

    query_mock.side_effect = _query_mock_side_effect
    test_config.change_working_directory(tmp_path)
    test_start_key = 'https://data.asc-csa.gc.ca/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/'
    test_config.data_sources = [test_start_key]
    test_config.data_source_extensions = ['.fits']
    test_start_time_str = '2022-02-01T13:57:00'
    State.write_bookmark(test_config.state_fqn, test_start_key, make_datetime(test_start_time_str))

    session_mock = Mock()

    class NeossatPagesTemplate(html_data_source.HtmlFilteredPagesTemplate):

        def __init__(self, config):
            super().__init__(config)

        def add_children(self, to_node, in_tree, new_entries):
            # which template_filter gets added
            if in_tree.parent(to_node.identifier).is_root():
                template_filter = self._always_true_filter
            elif in_tree.parent(to_node.identifier).is_leaf():
                template_filter = self._always_true_filter
            else:
                template_filter = self._file_filter

            for url in new_entries:
                in_tree.create_node(url, parent=to_node.identifier, data=template_filter)

        def is_leaf(self, url_tree, url_node):
            return url_tree.depth(url_node) == 3

    test_html_filters = NeossatPagesTemplate(test_config)
    test_subject = html_data_source.HttpDataSource(test_config, test_start_key, test_html_filters, session_mock)
    assert test_subject is not None, 'ctor failure'

    test_subject.initialize_start_dt()
    assert test_subject.start_dt == datetime(2022, 2, 1, 13, 57), 'wrong start time'

    test_subject.initialize_end_dt()
    assert test_subject.end_dt == datetime(2022, 8, 22, 15, 30), 'wrong end time'

    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a result'
    assert len(test_result) == 420, 'wrong number of results'

    first_result = test_result.popleft()
    assert (
        first_result.entry_name == 'https://data.asc-csa.gc.ca/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/2017'
        '/001/NEOS_SCI_2022001030508.fits'
    ), 'wrong first url'
    assert first_result.entry_dt == datetime(2022, 3, 1, 13, 56), 'wrong first time'

    last_result = test_result.pop()
    assert (
        last_result.entry_name
        == 'https://data.asc-csa.gc.ca/users/OpenData_DonneesOuvertes/pub/NEOSSAT/ASTRO/NESS/313/'
        'NEOS_SCI_2022001081159.fits'
    ), 'wrong last url'
    assert last_result.entry_dt == datetime(2022, 8, 22, 15, 30), 'wrong last time'

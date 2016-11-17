""" Tests of basic CLI interaction with ES w.r.t. Index operations.

The basic Index operations that Elasticsearch provides are creation, deletion,
existence check, and request for metadata-like information (i.e.,
settings, mappings, and aliases); these tests validate that functionality
for our CLI and also serve as a basic validation of our communication with ES.

 """

import os
import subprocess

import pytest

import bin
from tests.conftest import count_prefixed_indices, make_index_name


__author__ = "Vince Reuter"
__modified__ = "2016-11-17"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_index_operations"


ESPROV_PATH = os.path.join(os.path.dirname(bin.__file__), "esprov")
DEFAULT_TEST_INDEX_NAME = "simpletest"


class TestIndexCreation:
    """ Tests for creation of an Elasticsearch Index. """


    @staticmethod
    def build_index(name=DEFAULT_TEST_INDEX_NAME):
        """
        Build Elasticsearch index with given name

        :param str name: raw name for index
            (will be prefixed with test-indicative text)
        """
        cmd = [ESPROV_PATH, "index", "create", make_index_name(name)]
        subprocess.call(cmd)


    def test_create_single_index(self, es_client):
        """ Most basic index creation case is single index. """
        self.build_index()
        assert 1 == count_prefixed_indices(es_client)


    @pytest.mark.skip()
    def test_create_multiple_indices(self, es_client):
        """ Multiple indices may be created for the same client. """
        first_index_name = "first_index"
        self.build_index(first_index_name)
        expected_index_names = {first_index_name}
        observed_index_names = set(es_client.indices.get_alias().keys())
        assert expected_index_names == observed_index_names



class TestDeleteIndex:
    """ Tests for deletion of an Elasticsearch Index. """
    pass



class TestIndexExistence:
    """ Tests for test of existence of an Elasticsearch Index. """
    pass



class TestIndexFetch:
    """ Tests for fetch of information about an Elasticsearch Index. """
    pass

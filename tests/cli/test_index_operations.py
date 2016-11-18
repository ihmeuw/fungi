""" Tests of basic CLI interaction with ES w.r.t. Index operations.

The basic Index operations that Elasticsearch provides are creation, deletion,
existence check, and request for metadata-like information (i.e.,
settings, mappings, and aliases); these tests validate that functionality
for our CLI and also serve as a basic validation of our communication with ES.

 """

import os

import pytest

import bin
from bin import cli
from tests.conftest import count_prefixed_indices, make_index_name, ES_CLIENT


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
    def build_index(
                client=ES_CLIENT,
                name=DEFAULT_TEST_INDEX_NAME,
                parser=cli.CLIFactory.get_parser()
    ):
        """
        Build Elasticsearch index with name for client.

        :param client: Elasticsearch client, optional
        :param str name: raw name for index;
            this will be prefixed with test-indicative text, optional
        :param argparse.ArgumentParser parser: command-line
            argument parser, optional
        """
        args = parser.parse_args(["index", "insert", make_index_name(name)])
        cli.index(client, args)


    def test_create_single_index(self, es_client):
        """ Most basic index creation case is single index. """
        self.build_index()

        # DEBUG
        try:
            assert 1 == count_prefixed_indices(es_client)
        except AssertionError as e:
            print "INDEX NAMES: {}".format(es_client.indices.get_alias().keys())
            raise e


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

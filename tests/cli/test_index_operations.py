""" Tests of basic CLI interaction with ES w.r.t. Index operations.

The basic Index operations that Elasticsearch provides are creation, deletion,
existence check, and request for metadata-like information (i.e.,
settings, mappings, and aliases); these tests validate that functionality
for our CLI and also serve as a basic validation of our communication with ES.

 """

import os

import bin
from bin import cli
from tests.conftest import \
    count_prefixed_indices, get_prefixed_indices, \
    make_index_name, ES_CLIENT, TEST_INDEX_PREFIX


__author__ = "Vince Reuter"
__modified__ = "2016-11-17"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_index_operations"


ESPROV_PATH = os.path.join(os.path.dirname(bin.__file__), "esprov")
DEFAULT_TEST_INDEX_NAME = "{}-simpletest".format(TEST_INDEX_PREFIX)



class TestIndexCreation:
    """ Tests for creation of an Elasticsearch Index. """


    @staticmethod
    def build_index(
                name=DEFAULT_TEST_INDEX_NAME,
                client=ES_CLIENT,
                parser=cli.CLIFactory.get_parser()
    ):
        """
        Build Elasticsearch index with name for client.

        :param str name: name for ES Index to build;
            this should begin with designated prefix for test index names
        :param client: Elasticsearch client, optional
        :param argparse.ArgumentParser parser: command-line
            argument parser, optional
        :raises ValueError: if name for test index to construct
            does not begin with the designated test index name prefix
        """

        # Self-protection; require test-indicative name prefix.
        if not name.startswith(TEST_INDEX_PREFIX):
            raise ValueError(
                    "Name for ES Index in test context "
                    "should begin with {}; got {}".
                    format(TEST_INDEX_PREFIX, name)
            )

        # Parse arguments for Index construction and execute.
        args = parser.parse_args(["index", "insert", name])
        cli.index(client, args)


    def test_create_single_index(self, es_client):
        """ Most basic index creation case is single index. """
        self.build_index()
        assert 1 == count_prefixed_indices(es_client)


    def test_create_multiple_indices(self, es_client):
        """ Multiple indices may be created for the same client. """

        first_index_name = make_index_name("first_index")
        self.build_index(first_index_name, client=es_client)
        expected_index_names = {first_index_name}
        observed_index_names = get_prefixed_indices(client=es_client)
        assert expected_index_names == observed_index_names

        second_index_name = make_index_name("second_index")
        self.build_index(second_index_name, client=es_client)
        expected_index_names = {first_index_name, second_index_name}
        observed_index_names = get_prefixed_indices(client=es_client)
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

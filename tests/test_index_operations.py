""" Tests of basic CLI interaction with ES w.r.t. Index operations.

The basic Index operations that Elasticsearch provides are creation, deletion,
existence check, and request for metadata-like information (i.e.,
settings, mappings, and aliases); these tests validate that functionality
for our CLI and also serve as a basic validation of our communication with ES.

 """

import os

import pytest

import bin
from esprov.functions import \
    INDEX_CREATION_NAMES, INDEX_DELETION_NAMES
from tests.conftest import \
    call_cli_func, count_prefixed_indices, get_prefixed_indices, \
    make_index_name, DEFAULT_TEST_INDEX_NAME, ES_CLIENT, TEST_INDEX_PREFIX


__author__ = "Vince Reuter"
__modified__ = "2016-11-19"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_index_operations"


ESPROV_PATH = os.path.join(os.path.dirname(bin.__file__), "esprov")



class TestUnsupported:
    """ Tests for unsupported operations. """


    @pytest.fixture(scope="function", params=["unsupported"])
    def unsupported_index_operation_name(self, request):
        """
        Create and return unsupported Index operation name (undefined in CLI)

        :param pytest._pytest.fixtures.FixtureRequest request: test case
            function requesting parameterization
        :return str: unsupported Index operation name
        """
        return request.param


    def test_unsupported_index_operation_subcommand(
            self, inserted_index_and_response, unsupported_index_operation_name
    ):
        """ Attempt to use unsupported subcommand is erroneous/exceptional. """

        # Insert dummy test Index into default ES client and assert existence.
        index, response = inserted_index_and_response
        assert index in ES_CLIENT.indices.get_alias().keys()

        command = "index {} {}".format(unsupported_index_operation_name, index)

        # Attempt to execute unsupported subcommand fails on argparse.
        with pytest.raises(SystemExit):
            call_cli_func(command)



class TestIndexCreation:
    """ Tests for creation of an Elasticsearch Index. """


    @pytest.fixture(scope="function", params=list(INDEX_CREATION_NAMES))
    def client_and_opname(self, request, es_client):
        """ Provide full space of Index creation aliases. """
        return es_client, request.param


    @staticmethod
    def build_index(operation, name=DEFAULT_TEST_INDEX_NAME, client=ES_CLIENT):
        """
        Build Elasticsearch index with name for client.

        :param str operation: specific name for operation for Index creation
        :param str name: name for ES Index to build;
            this should begin with designated prefix for test index names
        :param elasticsearch.client.Elasticsearch client: ES client
            to use for Index construction
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
        call_cli_func("index {op} {index}".format(op=operation, index=name),
                      client=client)


    def test_create_single_index(self, client_and_opname):
        """ Most basic index creation case is single index. """
        client, opname = client_and_opname
        self.build_index(operation=opname)
        assert 1 == count_prefixed_indices(client)


    def test_create_multiple_indices(self, client_and_opname):
        """ Multiple indices may be created for the same client. """

        client, opname = client_and_opname

        first_index_name = make_index_name("first_index")
        self.build_index(operation=opname,
                         name=first_index_name,
                         client=client)
        expected_index_names = {first_index_name}
        observed_index_names = get_prefixed_indices(client=client)
        assert expected_index_names == observed_index_names

        second_index_name = make_index_name("second_index")
        self.build_index(operation=opname,
                         name=second_index_name,
                         client=client)
        expected_index_names = {first_index_name, second_index_name}
        observed_index_names = get_prefixed_indices(client=client)
        assert expected_index_names == observed_index_names



class TestIndexDeletion:
    """ Tests for deletion of an Elasticsearch Index. """


    @pytest.fixture(scope="function", params=list(INDEX_DELETION_NAMES))
    def client_and_opname(self, request, es_client):
        """ Provide full space of Index deletion aliases. """
        return es_client, request.param


    def test_remove_nonexistent(self, client_and_opname):
        """ Attempt to remove nonexistent index is fine, no effect. """
        client, opname = client_and_opname
        call_cli_func("index {} do_not_build".format(opname), client=client)


    def test_remove_extant(self, client_and_opname,
                           inserted_index_and_response):
        """ Test removal of index known to ES client. """

        client, opname = client_and_opname

        # Insert index and validate insertion.
        index, response = inserted_index_and_response
        assert index in client.indices.get_alias().keys()

        call_cli_func("index {} {}".format(opname, index), client=client)
        assert index not in client.indices.get_alias().keys()



class TestIndexExistence:
    """ Tests for test of existence of an Elasticsearch Index. """


    def test_index_does_exist(self, es_client, inserted_index_and_response):
        """ When Index with given name is known, existence is True. """
        index, response = inserted_index_and_response
        assert index in es_client.indices.get_alias().keys()
        assert call_cli_func("index exists {}".format(index))


    def test_index_does_not_exist(self, es_client,
                                  inserted_index_and_response):
        """ When Index with given name is unknown, existence is False. """
        index, response = inserted_index_and_response
        assert index in es_client.indices.get_alias().keys()
        unknown_index = "{}_unknown_suffix".format(index)
        assert not call_cli_func("index exists {}".format(unknown_index))

""" General test configuration/setup/constants. """

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

from elasticsearch import Elasticsearch
import pytest

from esprov import HOST, PORT
from utils import *

__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.conftest"


TEST_INDEX_SEARCH_STRING = "{}*".format(TEST_INDEX_PREFIX)


ES_CLIENT = Elasticsearch(hosts=[{"host": HOST, "port": PORT}])


def count_test_index_names(client=ES_CLIENT):
    """
    Count the number of esprov test prefix-named indices.

    :param elasticsearch.client.Elasticsearch client: ES client
    :return int: number of client's indices named with esprov prefix.
    """
    return len(client.indices.get_alias(index=TEST_INDEX_SEARCH_STRING))


def assert_no_test_indices(client=ES_CLIENT):
    """
    Assert that no test-prefix-named index is known to given client.

    :param elasticsearch.client.Elasticsearch client: ES client
    :raises AssertionError: if client has least one test-prefix-named index
    """
    assert 0 == count_test_index_names(client)


@pytest.fixture(scope="function")
def es_client(request):
    """
    Provide test function with ES client, clearing all indices when done

    :param pytest._pytest.fixtures.FixtureRequest request: test function
        requesting fixture
    """

    # Ensure that the test begins with no preexisting index.
    assert_no_test_indices()

    def clear():
        """ Upon test conclusion, clear all indices named by
        esprov test convention; assert none are lingering. """
        # Perform the deletion.
        request.client.indices.delete(index=TEST_INDEX_SEARCH_STRING,
                                      ignore=[400, 404])
        # Check that no test indices remain.
        assert_no_test_indices()

    # Provide requesting test function with teardown.
    request.addfinalizer(clear)

    return ES_CLIENT

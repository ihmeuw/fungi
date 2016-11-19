""" General test configuration/setup/constants. """

import json
import logging
import subprocess

from elasticsearch import Elasticsearch
import pytest

from esprov import HOST, PORT

__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.conftest"


TEST_INDEX_PREFIX = "esprov-test"
TEST_INDEX_SEARCH_STRING = "{}*".format(TEST_INDEX_PREFIX)
DEFAULT_TEST_INDEX_NAME = "{}-simpletest".format(TEST_INDEX_PREFIX)

ES_CLIENT = Elasticsearch(hosts=[{"host": HOST, "port": PORT}])
ES_URL_BASE = "http://{h}:{p}".format(h=HOST, p=PORT)

LOGGER = logging.getLogger(__modname__)



def assert_no_test_indices(client=ES_CLIENT):
    """
    Assert that no test-prefix-named index is known to given client.

    :param elasticsearch.client.Elasticsearch client: ES client
    :raises AssertionError: if client has least one test-prefix-named index
    """
    assert 0 == count_prefixed_indices(client)



def clear_test_indices():
    """ Upon test conclusion, clear all indices named by
    esprov test convention; assert none are lingering. """
    remove_test_indices(TEST_INDEX_SEARCH_STRING)
    assert_no_test_indices()



@pytest.fixture(scope="function")
def es_client(request):
    """
    Provide test function with ES client, clearing all indices when done

    :param pytest._pytest.fixtures.FixtureRequest request: test function
        requesting fixture
    """

    # Ensure that the test begins with no preexisting index.
    assert_no_test_indices()

    # Provide requesting test function with teardown.
    request.addfinalizer(clear_test_indices)

    return ES_CLIENT



@pytest.fixture(scope="function")
def inserted_index_and_response(request):
    """
    Insert a default test index into the default ES client.

    :return str, dict: inserted Index name and mapping obtained
        from parsing response as JSON
    """

    # Build command.
    index_name = DEFAULT_TEST_INDEX_NAME
    command = "curl -XPUT {es}/{index}".format(es=ES_URL_BASE,
                                               index=index_name)

    # Execute command and capture output.
    proc = subprocess.Popen(_subprocessify(command), stdout=subprocess.PIPE)
    response = proc.stdout.read()

    # Add cleanup function for test case and parse output.
    request.addfinalizer(clear_test_indices)
    return index_name, json.loads(response)



def remove_test_indices(index_names_wildcard_expression):
    """
    Clear client's test indices (names prefixed)

    :param str index_names_wildcard_expression: wildcard-containing
        expression to use in order to match index names for removal
    """
    es_url = "{es}/{indices_wildcard}?pretty&pretty".format(
        es=ES_URL_BASE, indices_wildcard=index_names_wildcard_expression
    )
    command_elements = _subprocessify("curl -XDELETE {}".format(es_url))
    subprocess.check_call(command_elements)



def make_index_name(suffix):
    """
    Create index name for ES unit test(s) with fixed prefix and given suffix.

    :param str | int suffix: suffix for ES index to create
    :return str: index name for ES unit test(s),
        with fixed prefix and given suffix
    """
    return "{}-{}".format(TEST_INDEX_PREFIX, suffix)



def valid_index_count(client, expected_match_count, prefix=TEST_INDEX_PREFIX):
    """
    Count up index names with given prefix known to client and compare that
    count with an expected value; return flag indicating con/discordance.

    :param elasticsearch.client.Elasticsearch client:
    :param int expected_match_count: number of ES client
        index names expected to begin with the given prefix
    :param str prefix: expected ES prefix; optional, default to module constant
    :return bool: flag indicating con/discordance between
        expected and observed count
    """
    prefix = _trim_prefix(prefix)
    return expected_match_count == count_prefixed_indices(client, prefix)



def valid_index_names(client, expected_index_names, prefix=TEST_INDEX_PREFIX):
    """
    Determine client's knowledge of ES indices matches expectation with
    respect to names prefixed as specified.

    :param elasticsearch.client.Elasticsearch client: ES client
        on which to validate index names
    :param iterable(str) expected_index_names: exact collection of index names
        with given prefix that the given client is expected to be aware
    :param str prefix: beginning of names of indices of interest;
        optional, will fall back on module-scope constant
    :return bool: flag indicating whether the expectation is satisfied
    """

    # Prefix may inadvertently be given in a glob-/wildcard-like format.
    prefix = _trim_prefix(prefix)

    all_known_index_names = client.indices.get_alias().keys()
    matched_index_names = {index_name for index_name in all_known_index_names
                           if index_name.startswith(prefix)}

    return set(expected_index_names) == matched_index_names



def get_prefixed_indices(client, prefix=TEST_INDEX_PREFIX):
    """
    Get index names with given prefix that are known to client.

    :param elasticsearch.client.Elasticsearch client: Elasticsearch client
        from which to get matching index names
    :param str prefix: prefix on which to match index names;
        optional, if omitted module-scope constant will be used
    :return set[str]: collection of index names with
        given prefix that are known to client
    """
    return {index_name for index_name in client.indices.get_alias().keys()
            if index_name.startswith(prefix)}



def count_prefixed_indices(client, prefix=TEST_INDEX_PREFIX):
    """
    Count the number of indices with prefix of which client is aware.

    :param elasticsearch.client.Elasticsearch client: Elasticsearch client
        on which to count matching index names
    :param str prefix: prefix on which to match index names;
        optional, if omitted module-scope constant will be used
    :return int: number of indices with prefix of which client is aware
    """
    return len(get_prefixed_indices(client, prefix))


def _subprocessify(command_text):
    return command_text.split(" ")


def _trim_prefix(prefix):
    return prefix[:-1] if prefix.endswith('*') else prefix

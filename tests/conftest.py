""" General test configuration/setup/constants. """

import abc
import json
import logging
import subprocess

import elasticsearch
from elasticsearch_dsl import connections
import pytest

from bin import cli
from esprov import CODE_STAGE_NAMESPACE_PREFIX, HOST, NAMESPACE_DELIMITER, PORT
from esprov.functions import LIST_STAGES_TIMESPANS
from esprov.provda_record import ProvdaRecord

__author__ = "Vince Reuter"
__modified__ = "2016-11-21"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.conftest"


CLI_FORMAT_NAME = "cli"
RAW_FORMAT_NAME = "raw"
OUTPUT_FORMAT_NAMES = [CLI_FORMAT_NAME, RAW_FORMAT_NAME]

TEST_INDEX_PREFIX = "esprov-test"
TEST_INDEX_SEARCH_STRING = "{}*".format(TEST_INDEX_PREFIX)
DEFAULT_TEST_INDEX_NAME = "{}-simpletest".format(TEST_INDEX_PREFIX)

ES_CLIENT = elasticsearch.Elasticsearch(hosts=[{"host": HOST, "port": PORT}])
ES_URL_BASE = "http://{h}:{p}".format(h=HOST, p=PORT)
TEST_CLIENT_NAME = "test_es_client"
connections.connections.add_connection(TEST_CLIENT_NAME, ES_CLIENT)
connections.connections.add_connection("default", ES_CLIENT)


LOGGER = logging.getLogger(__modname__)



@pytest.fixture(scope="function", params=list(LIST_STAGES_TIMESPANS))
def time_lag(request):
    """ Parameterize test case with time lag such that
     there stage(s) is/are in-bounds for a query. """
    # TODO: implement this for accuracy tests to test specific function(s).
    return request.param



class RawAndCliValidator:
    """ Abstract base for test classes this sort of validation:
     'for a given expectation, validate both the raw and CLI output versions.'
     This provides that validation method, but each child class must define
     the function with which to transform raw output to CLI-like format."""

    __metaclass__ = abc.ABCMeta


    def validate(self, expected, observed, output_format):
        """
        Validate equality expectation between expectation and observation,
        formatting each if necessary as indicated by output_format.

        :param collections.abc.Iterable expected: collection of expected
            results, possibly to be transformed
        :param collections.abc.Iterable observed: collection of
            observed results, possibly to be transformed
        :param str output_format: text indicating call version of function
            under test, implying output format
        :raises ValueError: if output format indicated is unknown/unsupported
        """
        if output_format == CLI_FORMAT_NAME:
            assert self.format_output(expected) == self.format_output(observed)
        elif output_format == RAW_FORMAT_NAME:
            assert expected == observed
        else:
            raise ValueError("Unsupported output format: {} ({})".
                             format(output_format, type(output_format)))


    @abc.abstractmethod
    def format_output(self, results):
        """
        Method with which to transform collection of results for
        CLI-version of call and output; each (concrete) subclass must
        supply an implementation.

        :param collections.abc.Iterable results: collection of
            results to reformat
        :return object: determined by subclass implementation
        """
        pass



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



def call_cli_func(command, client=ES_CLIENT):
    """
    Call a CLI function based on given command and with given ES client.
    This should ALWAYS MATCH the parse-and-call pattern that's used in the
    actual esprov executable (bin/esprov).

    :param str command: text as would be entered at a command prompt
    :param elasticsearch.client.Elasticsearch client: client to use for ES call
    :return NoneTye | object: null for some commands, legitimate value
        for functions for which return value is meaningful, e.g. bool
        for Index existence check
    """
    args = cli.CLIFactory.get_parser().parse_args(command.split(" "))
    logging.debug("Making CLI function call based on parsed arguments %s",
                  str(args))
    return args.func(client, args)



@pytest.fixture(scope="function", params=OUTPUT_FORMAT_NAMES)
def output_format(request):
    """
    Parameterize test case function with indicator of the output format;
    this determines execution via CLI-simulating subprocess vs.
    the function defined in the CLI module.

    :param pytest._pytest.fixtures.FixtureRequest request: test case function
        requesting parameterization
    :return str: indicator of way in which to invoke function under test,
        and therefor an indicator of the output format
    """
    return request.param



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

    # TODO: use Popen.communicate() instead; see "Note" on:
    # https://docs.python.org/2/library/subprocess.html#subprocess.check_call
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


def code_stage_text(stage_name):
    """
    Form a provda-compliant code stage name from given name.

    :param str stage_name: name for processing stage (e.g., a script's path)
    :return str: provda-compliant code stage name from given name
    """
    return "{prefix}{delim}{name}".\
        format(prefix=CODE_STAGE_NAMESPACE_PREFIX,
               delim=NAMESPACE_DELIMITER, name=stage_name)


def upload_records(client, records_by_index,
                   index_name=DEFAULT_TEST_INDEX_NAME):
    """
    Upload records by index to Elasticsearch client.

    :param elasticsearch.client.Elasticsearch client: ES client
        for document upload
    :param collections.abc.Mapping[str, collections.abc.Iterable] |
        collections.abc.Iterable records_by_index: ideally, a mapping from
        index name to collection of records to insert; alternatively, this
        could be provided as a simple collection of records, in which case
        the records will be inserted into a default-named index.
    :param str index_name: name for index in which to insert
        the records; optional, useful for when records_by_index is
        an ordinary collection rather than a mapping, but there's a default
    """

    try:
        # Assume that records are given as a mapping.
        records_by_index = records_by_index.items()
    except AttributeError:
        # Perhaps records were given as simple collection rather than mapping.
        # Use default Index name.
        records_by_index = {index_name: records_by_index}

    for index_name, records in records_by_index.items():
        # Establish the provda record mapping for current index within client.
        ProvdaRecord.init(index=index_name)

        for record in records:
            # Create and store document for current record.
            success = ProvdaRecord(**record).save(index=index_name,
                                                  validate=False)
            logging.debug(
                    "%s -- %s uploaded to index %s: %s",
                    "SUCCESS" if success else "FAILURE",
                    "WAS" if success else "NOT",
                    index_name, str(record)
            )



def parse_records_text(record_texts):
    """
    Transform collection of mappings-encoding text into mappings collection.

    :param iterable(str) record_texts: collection of records text
    :return iterable(dict): collection of mappings parsed from text
    """
    return [json.loads(record_text) for record_text in record_texts]



def _subprocessify(command_text):
    """
    Convert command-line command text into form for subprocess call.

    :param command_text: command-line text command
    :return list[str]: command text ready to be used
        as argument for subprocess call
    """
    return command_text.split(" ")



def _trim_prefix(prefix):
    return prefix[:-1] if prefix.endswith('*') else prefix

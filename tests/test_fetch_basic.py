""" Tests for the basic record-fetching functionality """

from elasticsearch import Elasticsearch
import pytest

from bin import cli
from conftest import call_cli_func, ES_CLIENT
import esprov
from esprov import functions, DOCUMENT_TYPENAMES


__author__ = "Vince Reuter"
__modified__ = "2016-11-09"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "provda_sandbox.tests.functions.test_fetch_basic"



@pytest.fixture(scope="function", params=esprov.DOCUMENT_TYPENAMES)
def known_doctype(request):
    """
    Parameterize a test case with name of a known document type.

    :param pytest._pytest.fixtures.FixtureRequest request: test case
        that is requesting parameterization
    :return str: parameter for requesting test case
    """
    return request.param


@pytest.fixture(scope="function")
def es_client(request):
    """
    Provide requesting test case with an Elasticsearch query client.

    :param pytest._pytest.fixtures.FixtureRequest request: test case
        requesting an Elasticsearch client instance
    :return elasticsearch.Elasticsearch: Elasticsearch client instance
    """
    return Elasticsearch()


class TestBasicFetch:
    """ Tests for basic provenance record fetching functionality """
    # Note that in some of these situations, due to lazy generator evaluation,
    # we need to have a dummy call like list() in order to force evaluation.
    # This is needed even to make an assertion about exception raising.
    # TODO: test on-the-fly capability.
    # TODO: note that fetch() supports -i/--index, -n/--num_docs, and --doctype.

    DOCTYPE_BASE = "fetch --doctype{}"


    def test_missing_doctype(self):
        with pytest.raises(SystemExit):
            # Here, exception is in parsing, so we need no evaluation forcing.
            call_cli_func(command=self.DOCTYPE_BASE.format(""))


    @pytest.mark.parametrize(argnames="invalid_doctype",
                             argvalues=["", " ", "  ", ])
    def test_invalid_empty_doctypes(self, invalid_doctype):
        """ Empty/whitespace doctype causes ValueError in document fetcher. """
        args = cli.CLIFactory.get_parser().parse_args(["fetch"])
        setattr(args, "doctype", invalid_doctype)
        with pytest.raises(ValueError):
            list(functions.fetch(ES_CLIENT, args))


    def test_invalid_nonempty_doctype(self):
        """ Invalid doctype causes a ValueError in document fetcher. """
        command = "fetch --doctype invalid_doctype"
        with pytest.raises(ValueError):
            list(call_cli_func(command))


    def test_known_doctype(self, known_doctype):
        pass


    def test_specific_index(self, known_doctype):
        pass


    def test_hit_limit(self, known_doctype):
        pass

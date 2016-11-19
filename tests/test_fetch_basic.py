""" Tests for the basic record-fetching functionality """

from elasticsearch import Elasticsearch
import pytest

from bin import cli
from esprov import DOCUMENT_TYPENAMES

__author__ = "Vince Reuter"
__modified__ = "2016-11-09"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "provda_sandbox.tests.functions.test_fetch_basic"



@pytest.fixture(scope="function", params=DOCUMENT_TYPENAMES)
def known_doctype(request):
    """
    Parameterize a test case with name of a known document type.

    :param pytest._pytest.fixtures.FixtureRequest request: test case
        that is requesting parameterization
    :return str: parameter for requesting test case
    """
    return request.param


@pytest.fixture(scope="function", params=[None, "", "invalid_doctype"])
def unknown_doctype(request):
    """
    Parameterize a test case with unknown document type.

    :param pytest._pytest.fixtures.FixtureRequest request: test case
        that is requesting parameterization
    :return NoneType | str: parameter for requesting test case
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

    # TODO: test on-the-fly capability.

    def test_unknown_doctype(self, es_client, unknown_doctype):
        with pytest.raises(ValueError):
            cli.fetch(es_client, unknown_doctype)


    def test_known_doctype(self, known_doctype):
        pass


    def test_specific_index(self, known_doctype):
        pass


    def test_hit_limit(self, known_doctype):
        pass

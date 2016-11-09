""" Executable for working with provenance-oriented data in Elasticsearch. """

import argparse

from elasticsearch_dsl import Search

import esprov
from esprov import DOCTYPE_KEY_PREFIX

__author__ = "Vince Reuter"
__modified__ = "2016-11-07"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "provda_sandbox.bin.cli"


# Relevant components of query that is sent to ES:
# 1 -- "Query" (JSON object)
# 2 -- "Size"
# 3 -- "Fields" (document fields to include in result) -->
# TODO: from each hit, or from result object?

# Others, unsupported here
# 1 -- "From" --> Offset into results (default 0)
# 2 -- "Facets" --> Summary information about specific field(s) in data
# 3 -- "Filter" -- > Special case for when you want to apply filter/query to results not to facets


QUERY_FUNCTION_NAMES = {"fetch"}


def fetch(es_client, doctype, num_docs, **match_kwargs):
    """
    Perform Elasticsearch TERM-level query, fetching matching documents.

    :param elasticsearch.Elasticsearch es_client: Elasticsearch client
        to use for query
    :param str doctype: name for type of document(s) to fetch
    :param int num_docs: maximum number of document hits to return
    :param dict match_kwargs: keywords and arguments to use in query
    :raises ValueError: if doctype is an unknown
    """
    # TODO: finish docstring.
    # Entity options: activity, agent, entity
    # TODO: validate match kwargs based on doctype
    if doctype not in esprov.DOCUMENT_TYPENAMES:
        raise ValueError("Unknown doctype: {}".format(doctype))
    doctype_query_string = "{}{}".format(DOCTYPE_KEY_PREFIX, doctype)
    # TODO: properly construct Search instance.
    search = Search(using=es_client, match=match_kwargs)
    # TODO: determine type and properly filter result; are hits ordered by score?
    # TODO: improve efficiency here.
    # TODO: determine return type
    return list(search)[:num_docs]



def related():
    pass


class Subparser(object):
    """ Argument parser """
    def __init__(self, function, help_text, argnames):
        self.function = function
        self.help_text = help_text
        self.argnames = argnames


class CLIFactory(object):
    @classmethod
    def get_parser(cls):
        subparsers = (
            Subparser()
        )
        parser = argparse.ArgumentParser()
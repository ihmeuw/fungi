""" Ancillary functionality for provenance-in-Elasticsearch. """

import logging

from elasticsearch_dsl import Search

__author__ = "Vince Reuter"
__modified__ = "2016-11-15"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.utilities"


def build_search(es_client, index="_all"):
    """
    Build a search instance to execute for a CLI query.

    :param elasticsearch.client.Elasticsearch es_client: ES client with
        which to conduct the search query
    :param str | int index: ES index within which
        search query execution should be confined
    :return elasticsearch_dsl.search.Search: search instance to execute
    """
    search = Search(using=es_client, index=index)
    logging.debug("Search: {}".format(search.to_dict()))
    return search

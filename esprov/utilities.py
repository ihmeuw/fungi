""" Ancillary functionality for provenance-in-Elasticsearch. """

import logging

from elasticsearch_dsl import Search

__author__ = "Vince Reuter"
__modified__ = "2016-11-15"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.utilities"


def build_search(es_client, args):
    """
    Build a search instance to execute for a CLI query.

    :param elasticsearch.client.Elasticsearch es_client: ES client with
        which to conduct the search query
    :param argparse.Namespace args:
    :return elasticsearch_dsl.search.Search: search instance to execute
    """
    try:
        index = args.index
    except AttributeError:
        index = "_all"
    search = Search(using=es_client, index=index)
    logging.debug("Search: {}".format(search.to_dict()))
    return search

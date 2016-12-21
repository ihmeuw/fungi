""" Ancillary functionality for provenance-in-Elasticsearch. """

import itertools
import logging

from elasticsearch_dsl import Search

__author__ = "Vince Reuter"
__modified__ = "2016-11-15"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.utilities"


INVALID_ITEMS_LIMIT_EXCEPTION = ValueError


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



def capped(items, limit):
    """
    For given iterable

    :param collections.abc.Iterable items: collection of items from which to
        yield elements, up to number given by limit
    :param int limit: number of items
    :return gene:
    :raises ValueError: if given limit is non-numeric or less than 1
    """

    items = iter(items)

    if limit is None:
        logging.debug("No/null items limit specified; yielding all items...")
        return items

    limit = int(limit)

    if limit < 1:
        raise INVALID_ITEMS_LIMIT_EXCEPTION("Need positive items limit; "
                                            "got {}".format(limit))
    else:
        logging.debug("Yielding %d items", limit)
        return itertools.islice(items, limit)

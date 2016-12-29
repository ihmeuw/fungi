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


ITEMS_COUNT_LOWER_BOUND = 0



def parse_index(args):
    """
    This pattern is likely to be sufficiently ubiquitous to
    warrant a function to save redundancy.

    :param argparse.Namespace args: binding between option name and
        argument value
    :return str: text version of name(s) of index(es) to which
        to confine Elasticsearch query/operation
    """
    try:
        index = args.index
    except AttributeError:
        index = "_all"
    return index



def parse_num_docs(args):
    """
    Parse the cap for the number of hits to return from a document query.

    :param argparse.Namespace args: binding between option name and argument
    :return int: cap for the number of documents to return that hit on a query
    """
    default = 10
    try:
        return max(args.num_docs, default)
    except AttributeError:
        return default



def build_search(es_client, args):
    """
    Build a search instance to execute for a CLI query.

    :param elasticsearch.client.Elasticsearch es_client: ES client with
        which to conduct the search query
    :param argparse.Namespace args:
    :return elasticsearch_dsl.search.Search: search instance to execute
    """
    index = parse_index(args)
    search = Search(using=es_client, index=index)[:parse_num_docs(args)]
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
        raise IllegalItemsLimitException(limit, min=1)
    else:
        logging.debug("Yielding %d items", limit)
        return itertools.islice(items, limit)



class IllegalItemsLimitException(Exception):
    """ Represent case of illogical items count for iterable. """

    def __init__(self, limit, min=ITEMS_COUNT_LOWER_BOUND):
        reason = "Lower bound for items limit is {}; got {}".format(min, limit)
        super(IllegalItemsLimitException, self).__init__(reason)

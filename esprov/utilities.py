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



def finalize_results(results, num_records, delimiter=None):
    """
    Limit the results from a search query to the requested number of records.

    :param list results: ES search query results
    :param int num_records: number of records at which to cap results
    :param str delimiter: delimiter on which to join resulting elements, if
        text-ready result is what's desired (thus, optional)
    :return list | str: same as input, but (perhaps) pared down; if delimiter
        is given, result elements are joined on the delimiter for return
    :raises AttributeError: if delimiter is given but has no 'join' method;
        most obviously, this would occur if a non-string delimiter were given
    """

    if num_records is None:
        filtered = results
    elif num_records < 0:
        raise ValueError("Invalid maximum record count: {}".
                         format(num_records))
    else:
        logging.debug("Taking first %d of %d results, skipping %d",
                      num_records, len(results), len(results) - num_records)
        filtered = results[:num_records]

    return delimiter.join(filtered) if delimiter is not None else filtered

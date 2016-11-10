""" Provenance-in-Elasticsearch query functions. """

import logging

from elasticsearch_dsl import Search

from esprov import DOCTYPE_KEY, DOCUMENT_TYPENAMES

__author__ = "Vince Reuter"
__modified__ = "2016-11-09"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.functions"


__all__ = ["fetch"]


# This is the most basic search; one for document type
def fetch(es_client, args):
    """
    Perform Elasticsearch TERM-level query, fetching matching documents.

    :param elasticsearch.Elasticsearch es_client: Elasticsearch client
        to use for query
    :param argparse.Namespace args: arguments parsed from command line
    :return list[dict]: document hits from query, potentially capped
    :raises ValueError: if doctype given is unknown, or if document count
        limit is negative
    """

    funcpath = "{}.{}".format(__modname__, "fetch")
    logger = logging.getLogger(funcpath)
    logger.debug("In: {}".format(funcpath))

    # TODO: ensure docstring correctness.

    # First variety of fetch is for noun (agent, activity, entity).
    # The other is the fetch of a relationship.
    # Let's start with noun.
    # Entity options: activity, agent, entity

    # TODO: validate match kwargs based on doctype

    if args.doctype not in DOCUMENT_TYPENAMES:
        raise ValueError("Unknown doctype: {}".format(args.doctype))

    # TODO: properly construct Search instance.
    search = Search(using=es_client, index=args.index)
    logger.debug("Search: {}".format(search.to_dict()))

    # TODO: Properly filter result; are hits ordered by score?
    # TODO: empty query is logical here, but is it valid?

    result = \
        list(search.query("match",
                          **{DOCTYPE_KEY: args.doctype}).execute())

    if args.num_docs is None:
        return result
    if args.num_docs < 0:
        raise ValueError("Invalid max document count: {}".
                         format(args.num_docs))
    return result[:args.num_docs]

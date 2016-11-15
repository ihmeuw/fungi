""" Provenance-in-Elasticsearch query functions. """

import logging

from elasticsearch_dsl import Search

from esprov import DOCTYPE_KEY, DOCUMENT_TYPENAMES

__author__ = "Vince Reuter"
__modified__ = "2016-11-10"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.functions"


__all__ = ["fetch"]


# TODO: other use cases to handle and implement.
# 1 -- For file/DB (entity) (File in provda.process.rst?), agent(s) and/or process(es).
# 2 -- For agent, fetch influenced Process(es) or generated/used File(s).
# 3 -- For activity, fetch influenced activities or generated/used File(s).
# 4 -- Process collection stuff

# 1 --> wasGeneratedBy or used
# 2 --> wasAssociatedWith (from Process perspective)
# 3 -->
# 4 --> hadMember for component processes, then jump to process-level queries from there.
# This should inform the design decision for representing process collection.
# The design decision around process collection relates to whether a collection
# should be linked directly to entities as its components are (potentially more/redundant edges), and whether
# or not the components should be represented at all (more nodes).



def since():
    """ Determine the stages that have run within past given time """
    # TODO: start with assumption of days, then can add flexibility via optional arguments or other subfunctions.
    # TODO: the return should be the user script and affected directories or something thereabouts.

    pass



def modifier():
    """ Determine the most recent process/agent to modify file. """
    # TODO: want user and process, could do each individually/with required discrete argument, or with separate subfunctions.
    pass



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

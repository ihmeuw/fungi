""" Provenance-in-Elasticsearch query functions. """

import argparse
import logging

from esprov import \
    CODE_STAGE_NAMESPACE_PREFIX, DOCTYPE_KEY, \
    DOCUMENT_TYPENAMES, ID_ATTRIBUTE_NAME, TIMESTAMP_KEY
from esprov.utilities import build_search, finalize_results

__author__ = "Vince Reuter"
__modified__ = "2016-11-15"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.functions"


LIST_STAGES_PARAMETERS = ("months", "weeks", "days", "hours", "minutes")
ES_TIME_CHARACTERS = ('M', 'w', 'd', 'H', 'm')
TIME_CHAR_BY_CLI_PARAM = dict(zip(LIST_STAGES_PARAMETERS,
                                         ES_TIME_CHARACTERS))


__all__ = ["fetch", "list_stages", LIST_STAGES_PARAMETERS]


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


# TODO: completely segregate raw record strings from this module for minimal dependency and assumptions
# TODO: consider generalizing query formation procedure (there's a common mapping/unpacking pattern.)


# Cases for listing stages:
# 1 -- simplest: list a lag (interpret from current)
# The lag can have day, hour, minute components
# TODO: decide whether or not to require one of the lag components.
def list_stages(es_client, args):
    """
    Use given client to query Elasticsearch for documents
    at least as recent as time indicated by given arguments.

    :param elasticsearch.client.Elasticsearch es_client: client with
        which to conduct the Elasticsearch query
    :param argparse.Namespace args: binding between parameter name and
        argument value
    :return collections.abc.Iterable | str: iterable of results or
        print-ready version
    """

    """
    if all([arg is None for arg in LIST_STAGES_PARAMETERS]):
        raise TypeError(
            "To use list_stages, at least one of {} must be specified "
            "via the CLI.".format(", ".join(LIST_STAGES_PARAMETERS))
        )
    """

    # Build up the time text filter.
    if all([arg is None for arg in LIST_STAGES_PARAMETERS]):
        timespan_query_data = {}
    else:
        # Elasticsearch supports a time offset from current by
        # providing something like "now-1d-1H" to mean 25 hours earlier.
        time_text = "now"
        for time_param_name, es_time_char in TIME_CHAR_BY_CLI_PARAM.items():
            this_time_span_arg = getattr(args, time_param_name)
            if this_time_span_arg is not None:
                time_text += "-{}{}".format(this_time_span_arg,
                                            es_time_char)
        # Bind Elasticsearch timespan key-value pairs to the timestamp key.
        timespan_query_data = {TIMESTAMP_KEY: {"gte": time_text, "lt": "now"}}

    # Match on code document instances.
    record_type_query_data = {ID_ATTRIBUTE_NAME: CODE_STAGE_NAMESPACE_PREFIX}

    search = build_search(es_client, index=args.index)
    results = list(search.query("match", **record_type_query_data).
                   filter("range", timespan_query_data))

    return finalize_results(results, args.num_docs)



def list_stages_between(es_client, args, datetime_parser, datetime_writer):
    """


    :param es_client:
    :param args:
    :param datetime_parser:
    :param datetime_writer:
    :return:
    """
    pass


def list_stages_within(es_client, args, datetime_parser, datetime_writer):
    pass



def list_stages_since(es_client, args, datetime_parser, datetime_writer):
    """
    Determine the stages that have run within past given time.

    :param elasticsearch.client.Elasticsearch es_client: Elasticsearch client
        with which to execute the search query
    :param argparse.Namespace args: binding between parameter name and
        argument value
    :param function(str) -> datetime.datetime datetime_parser: function with
        which to parse text representation of datetime and produce datetime
    :param function(str) -> datetime.datetime datetime_writer: function with
        which to write text representation of datetime
    :return collections.abc.Iterable(str):
    """
    # TODO: finish docstring.
    # TODO: start with assumption of days, then can add flexibility via optional arguments or other subfunctions.
    # TODO: the return should be the user script and affected directories or something thereabouts.
    # There are relatively well-defined steps here.
    # Parse user input (determine/restrict datetime format.
    # CLI may allow its own sort of time format.
    # CLI should definitely support the native provda time format, though.
    # The native provda time format accords with RFC3339?
    # Perform any necessary translation between user input time and provda.
    # Form query for elasticsearch.
    # Send/execute query.
    # Parse and format result(s).

    # TODO: for the datetime range, ES provides support:
    # https://www.elastic.co/guide/en/elasticsearch/guide/current/_ranges.html.
    # The datetime format in the provda logs is: 2016-11-06T11:04:25.268Z
    # TODO: potential design upgrade: plugin functions
    # With plugins, custom datetime specification strategy could be used.
    pass



def list_modifiers(es_client, args):
    """ Determine the most recent process(es)/agent(s) to modify file/DB (entity). """
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

    # TODO: properly construct Search instance.
    search = build_search(es_client, args)

    # Assign the query mapping (bind doctype to match as needed.)
    if not args.doctype:
        # No doctype --> grab all records/documents.
        query_mapping = {}
    elif args.doctype not in DOCUMENT_TYPENAMES:
        # If doctype is given, it must be valid.
        raise ValueError("Unknown doctype: {}".format(args.doctype))
    else:
        # Doctype is given and is valid.
        query_mapping = {DOCTYPE_KEY: args.doctype}

    # TODO: Properly filter result; are hits ordered by score?
    # TODO: empty query is logical here, but is it valid?
    results = list(search.query("match", **query_mapping).execute())
    return finalize_results(results, num_records=args.num_docs)

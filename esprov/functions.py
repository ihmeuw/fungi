""" Provenance-in-Elasticsearch query functions. """

import argparse
import logging

from elasticsearch_dsl import Index

from esprov import \
    CODE_STAGE_NAMESPACE_PREFIX, DOCTYPE_KEY, \
    DOCUMENT_TYPENAMES, ID_ATTRIBUTE_NAME, TIMESTAMP_KEY
from esprov.provda_record import ProvdaRecord
from esprov.utilities import build_search

__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.functions"


# Related to list_stages
LIST_STAGES_TIMESPANS = ("months", "weeks", "days", "hours", "minutes")
ES_TIME_CHARACTERS = ('M', 'w', 'd', 'H', 'm')
TIME_CHAR_BY_CLI_PARAM = dict(zip(LIST_STAGES_TIMESPANS, ES_TIME_CHARACTERS))

# Related to Index operations
INDEX_CREATION_NAMES = {"insert", "create", "build"}
INDEX_DELETION_NAMES = {"remove", "delete"}
INDEX_EXISTENCE_NAMES = {"exists"}
INDEX_OPERATION_NAMES = \
    INDEX_EXISTENCE_NAMES | INDEX_CREATION_NAMES | INDEX_DELETION_NAMES

LOGGER = logging.getLogger(__modname__)


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
    :return elasticsearch_dsl.search.Search:
    """

    """
    if all([arg is None for arg in LIST_STAGES_TIMESPANS]):
        raise TypeError(
            "To use list_stages, at least one of {} must be specified "
            "via the CLI.".format(", ".join(LIST_STAGES_TIMESPANS))
        )
    """

    # TODO: support arguments about what other data in addition to stage names should be available.
    # TODO: also support specification of whether duplicate entries should be provided. (set/list uniquefaction)

    # Build up the time text filter.
    if all([arg is None for arg in LIST_STAGES_TIMESPANS]):
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

    search = build_search(es_client, args=args)
    query = search.query("match", **record_type_query_data)
    result = query.filter("range", **timespan_query_data)

    return result



def index(es_client, args):
    """
    Perform Elasticsearch Index-related
    operation indicated by subcommand's arguments.

    :param elasticsearch.client.Elasticsearch es_client: connection with
        which to create the new index.
    :param argparse.Namespace args: binding between parameter name
        and argument value for command-line options
    :return NoneType | bool: null for insert/remove; for existence check,
        a flag indicating whether client knows about an index with
        the specified name
    :raises ValueError: if namespace's index_operation is unknown
    """

    LOGGER.debug("Executing index operation...")
    operation_name = args.index_operation
    LOGGER.debug("Operation name is %s", str(operation_name))

    if operation_name in INDEX_CREATION_NAMES:
        LOGGER.debug("Inserting index %s", str(args.index_target))
        ProvdaRecord.init(index=args.index_target, using=es_client)
    elif operation_name in INDEX_DELETION_NAMES:
        LOGGER.debug("Removing index %s", str(args.index_target))
        # Ignore elasticsearch.exceptions.RequestError (400);
        # also ignore elasticsearch.exceptions.NotFoundError (404).
        es_client.indices.delete(index=args.index_target, ignore=[400, 404])
    elif operation_name in INDEX_EXISTENCE_NAMES:
        LOGGER.debug("Checking existence of index %s", str(args.index_target))
        exists = Index(args.index_target, using=es_client).exists()
        LOGGER.debug("{} existence: {} ({})".format(args.index_target,
                                                    exists, type(exists)))
        return exists
    else:
        raise ValueError("Unexpected index operation: {} ({})".
                         format(operation_name, type(operation_name)))



def fetch(es_client, args):
    """
    Perform Elasticsearch TERM-level query, fetching matching documents.

    :param elasticsearch.Elasticsearch es_client: Elasticsearch client
        to use for query
    :param argparse.Namespace args: arguments parsed from command line
    :return elasticsearch_dsl.search.Search: ES client Search instance
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

    search = build_search(es_client, args=args)

    # Assign the query mapping (bind doctype to match as needed.)
    if not args.doctype:
        # No doctype --> grab all records/documents.
        logger.debug("No doctype")
        query_mapping = {}
    elif args.doctype not in DOCUMENT_TYPENAMES:
        # If doctype is given, it must be valid.
        raise ValueError("Unknown doctype: {}".format(args.doctype))
    else:
        # Doctype is given and is valid.
        logger.debug("Doctype: %s", args.doctype)
        query_mapping = {DOCTYPE_KEY: args.doctype}

    # TODO: Properly filter result; are hits ordered by score?
    # TODO: empty query is logical here, but is it valid?
    # TODO: make use of the count() member of Search for testing.

    logger.debug("query_mapping: %s", str(query_mapping))
    result = search.query("match", **query_mapping)

    return result



def list_stages_TODO_stub(es_client, args, datetime_parser, datetime_writer):
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

""" Provenance-in-Elasticsearch query functions. """

import argparse
import logging

from elasticsearch_dsl import Index, Search

from esprov import \
    DOCTYPE_KEY, DOCUMENT_TYPENAMES, \
    ID_ATTRIBUTE_NAME, TIMESTAMP_KEY
from esprov.provda_record import ProvdaRecord
from esprov.utilities import build_search, capped, parse_index, parse_num_docs

__author__ = "Vince Reuter"
__modified__ = "2016-11-21"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.functions"


# Related to list_stages
MONTHS_NAME = "months"
LIST_STAGES_TIMESPANS = (MONTHS_NAME, "weeks", "days", "hours", "minutes")
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
# 1: For file/DB (entity) (File in provda.process.rst?), agent(s)/process(es).
# 2: For agent, fetch influenced Process(es) or generated/used File(s).
# 3: For activity, fetch influenced activities or generated/used File(s).
# 4: Process collection stuff

# 1: wasGeneratedBy or used
# 2: wasAssociatedWith (from Process perspective)
# 3: hadMember for component processes, then jump to process-level queries.
# This should inform the design decision for representing process collection.
# The design decision around process collection relates to whether a collection
# should be linked directly to entities as its components are
# (potentially more/redundant edges), and whether
# the components should be represented at all (more nodes).


# TODO: Separate raw record strings from this for min. dependency/assumption.
# TODO: consider generalizing query formation (common map/unpack pattern)


# Cases for listing stages:
# 1 -- simplest: list a lag (interpret from current)
# TODO: decide whether or not to require one of the lag components.
def list_stages(es_client, args):
    """
    Use given client to query Elasticsearch for documents
    at least as recent as time indicated by given arguments.
    Currently, supported options are time-lag-indicative ones with
    numeric argument, and the supported flag is "--id" to indicate
    that record ID is all that's needed, not the full record for a match.

    E.g., get ID for stages run within last 3.5 days
    ~ <User>$ esprov list_stages -id -d 3 -H 12

    :param elasticsearch.client.Elasticsearch es_client: client with
        which to conduct the Elasticsearch query
    :param argparse.Namespace args: binding between parameter name and
        argument value
    :return generator(str | dict): matches of time-based query, either as
        full record or as a distilled representation (like text hash-esque ID)
    """

    # TODO: incorporate this if it's decided to require >= 1 lag spec,
    # as it will effect ValueError if no time lag option/argument is given.
    """
    if all([arg is None for arg in LIST_STAGES_TIMESPANS]):
        raise ValueError(
            "To use list_stages, at least one of {} must be specified "
            "via the CLI.".format(", ".join(LIST_STAGES_TIMESPANS))
        )
    """

    assertion_statement = "Time lag span value must be nonnegative integer."

    # TODO 1: support extend this sort of thing beyond stages (other function)
    # TODO 2: support filter/reduction based on duplicates.

    if all([arg is None for arg in LIST_STAGES_TIMESPANS]):
        # Each supported timespan bound to null --> no time constraint.
        timespan_query_data = {}
    else:
        # Build up the time text filter.
        # Elasticsearch supports a time offset from current by providing
        # something like "now-1d-1H" to mean 25 hours before than now.

        # Native Elasticsearch format
        time_text = "now"

        # Consider each time lag span in turn, interrogating arguments parsed
        # from the command line for a value for that time lag span.
        for time_param_name, es_time_char in TIME_CHAR_BY_CLI_PARAM.items():
            this_time_span_arg = getattr(args, time_param_name)

            # Some (likely most/all) time lag span options won't be present.
            if this_time_span_arg is None:
                continue

            # Magnitude of lag must be nonnegative integer.
            try:
                assert int(this_time_span_arg) >= 0, assertion_statement
            except (AssertionError, ValueError, TypeError):
                raise ValueError(assertion_statement)

            # The Elasticsearch format is -<magnitude><lag character>
            time_text += "-{}{}".format(this_time_span_arg, es_time_char)

        # Bind Elasticsearch timespan key-value pairs to the timestamp key.
        # ES uses "now" to indicate current, and we want to filter to >= lag.
        timespan_query_data = {TIMESTAMP_KEY: {"gte": time_text, "lt": "now"}}

    # Match on code document instances.
    record_type_query_data = {DOCTYPE_KEY: "activity"}

    # Build and execute query.
    search = build_search(es_client, args=args)
    query = search.query("match", **record_type_query_data)

    # Filter result(s) based on timespan of interest.
    if timespan_query_data:
        result = query.filter("range", **timespan_query_data)
    else:
        result = query

    # Produce results as mappings rather than raw text or ADT instance.
    for response in result.scan():
        yield response[ID_ATTRIBUTE_NAME] if args.id else response.to_dict()



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
        # ProvdaRecord extends elasticsearch_dsl.document.DocType to define
        # mapping between incoming text record and provenance record fields.
        LOGGER.debug("Inserting index %s", str(args.index_target))
        ProvdaRecord.init(index=args.index_target, using=es_client)

    elif operation_name in INDEX_DELETION_NAMES:
        # Ignore elasticsearch.exceptions.RequestError (400);
        # also ignore elasticsearch.exceptions.NotFoundError (404).
        LOGGER.debug("Removing index %s", str(args.index_target))
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
    :param argparse.Namespace args: binding between option name
        and argument value
    :return generator(dict): documents matching the fetch request,
        with each document converted from raw text form to data mapping
    :raises ValueError: if doctype given is unknown, or if document count
        limit is negative, or if given index name matches no known index
    """

    funcpath = "{}.{}".format(__modname__, "fetch")
    logger = logging.getLogger(funcpath)
    logger.debug("In: {}".format(funcpath))

    search = build_search(es_client, args=args)

    # Assign mapping for query based on targeted doctype.
    if args.doctype is None:
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

    # Ensure that we're given a valid index.
    if args.index not in es_client.indices.get_alias() \
            and args.index != "_all":
        # "_all" is the fallback match-all index value.
        # That default applies when no index is given, so
        # by the time index argument is here, it should be set to
        # either the fallback/catch-all value or to a known index.
        raise ValueError("Unknown index: {} ({})".format(args.index,
                                                         type(args.index)))

    # TODO: Properly filter result; are hits ordered by score?
    # TODO: empty query is logical here, but is it valid?
    # TODO: make use of the count() member of Search for testing.

    if query_mapping:
        logger.debug("query_mapping: %s", str(query_mapping))
        result = search.query("match", **query_mapping)
        hits = result.scan()
    else:
        #logger.debug("Executing 'wildcard' query on %s field", DOCTYPE_KEY)
        #result = search.query("wildcard", **{DOCTYPE_KEY: "*"})
        logger.debug("No query mapping --> doing direct search...")
        resolved_index = parse_index(args)
        logger.debug("Parsed index name %s from %s", resolved_index, str(args))

        # DEBUG
        #logger.debug("USING EMPTY SEARCH FOR DEBUGGING")
        #hits = Search()

        # TODO: uncomment
        logger.debug("Searching index '%s'", resolved_index)
        #logger.debug("Client indices: %s", ", ".join(es_client.indices.get_alias().keys()))
        """
        logger.debug(
            "%s %s in %s",
            args.index,
            "IS" if args.index
                    in es_client.indices.get_alias().keys() else "NOT",
            ", ".join(es_client.indices.get_alias().keys())
        )
        """
        hits = Search(index=resolved_index)[:parse_num_docs(args)]
        #hits = result["hits"]["hits"]

        logger.debug("hits: %s (%s)", str(hits), type(hits))

    for hit in capped(items=hits, limit=args.num_docs):
        yield hit.to_dict()

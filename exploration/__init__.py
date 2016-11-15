""" Exploration of provenance via Logstash/Elasticsearch/NetworkX/Neo4j """

import itertools

from networkx.classes import MultiDiGraph

from esprov import DOCTYPE_KEY, DOCUMENT_KEY, LOGGER, PREFIX_TYPENAME

__author__ = "Vince Reuter"
__modified__ = "2016-11-10"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.exploration.__init__"


class DocumentMismatchException(Exception):
    """ Records allegedly from same document must match on document ID. """

    def __init__(self, expected, observed):
        message = "Expected document ID {} but got {}".format(expected,
                                                              observed)
        super(DocumentMismatchException, self).__init__(message)


def build_document(records):
    """
    Reconstruct provenance document from a collection of related records.

    :param collections.abc.Iterable(dict) records: collection of individual
        provenance records that together constitute a documents
    :return networkx.class.Digraph: directed graph representation of
        provenance document constructed from given records
    :raises TypeError: if records is null
    """

    records = iter(records)
    try:
        first_record = records.next()
    except StopIteration:
        return {}

    # For now, ignore "prefix" records.
    num_prefix_records = 0
    docid = first_record[DOCUMENT_KEY]

    graph = MultiDiGraph()

    for record in itertools.chain([first_record], records):

        if record[DOCTYPE_KEY] == PREFIX_TYPENAME:
            num_prefix_records += 1
            LOGGER.debug("Skipping prefix record %d", num_prefix_records)
            continue

        if record[DOCUMENT_KEY] != docid:
            raise DocumentMismatchException(docid, record[DOCUMENT_KEY])



    LOGGER.info("Skipped %d prefix records", num_prefix_records)


def build_documents(records):
    """


    :param records:
    :return:
    """
    pass

""" Package-level constants and setup for Elasticsearch-based provenance """

import logging
import sys

__author__ = "Vince Reuter"
__modified__ = "2016-11-19"
__modname__ = "esprov.esprov.__init__"


LOGGING_LEVEL = logging.DEBUG
LOGGING_MESSAGE_FORMAT = \
    "[%(asctime)s] {%(module)s:%(funcName)s:%(lineno)d} %(levelname)s - " \
    "%(message)s"


def setup_logger():
    logger = logging.getLogger(__modname__.split(".")[-2])
    logger.propagate = False
    logger.setLevel(LOGGING_LEVEL)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(LOGGING_LEVEL)
    handler.setFormatter(logging.Formatter(fmt=LOGGING_MESSAGE_FORMAT))
    logger.addHandler(handler)
    return logger


LOGGER = setup_logger()


# TODO: allow these to be configurable, favoring filepath (like airflow.cfg).
# TODO: user should be able to set these once, not need to do so for each call.
# Connect to Elasticsearch.
HOST = "localhost"
PORT = 9200

# Name for provenance document type within index(es) to search
DOCTYPE = "logs"

# Fields (keys) for a single document.
DOCTYPE_KEY = "prov"
MESSAGE_KEY = "@message"
TIMESTAMP_KEY = "@timestamp"
VERSION_KEY = "@version"
DOCUMENT_KEY = "document"
INSTANCE_KEY = "instance"
HOST_KEY = "@source_host"
HOSTNAME_KEY = "host"
PORT_KEY = "port"
FIELDS_KEY = "@fields"

# TODO: this should be configurable.
# TODO: there should be a document fieldname-to-relationship mapping
# TODO: design decision -- translation responsibility on client or internal?
# TODO: security concern -- if client maps names to relationships,
# TODO (cont.): the gate to malice via illogical mapping is opened
DOCUMENT_FIELDNAMES = {
    DOCTYPE_KEY, MESSAGE_KEY, TIMESTAMP_KEY,
    VERSION_KEY, DOCUMENT_KEY, INSTANCE_KEY,
    HOST_KEY, HOSTNAME_KEY, PORT_KEY, FIELDS_KEY
}

# TODO: An analogous point about configurability, security, and mapping
# TODO (continued): applies to the names for provenance relationships.
# TODO: consider allowing specification and mapping according to
# TODO (continued): enumerated type representing defined provenance standards.
DOCUMENT_TYPENAMES = {
    "activity",
    "agent",
    "entity",
    "hadMember",
    "prefix",
    "used",
    "wasAssociatedWith",
    "wasGeneratedBy",
    "wasInfluencedBy"
}


PREFIX_TYPENAME = "prefix"
DOCTYPE_KEY = "prov"
ID_ATTRIBUTE_NAME = "instance"
CODE_STAGE_NAMESPACE_PREFIX = "code"
NAMESPACE_DELIMITER = ":"


# TODO: consider putting this in nodes package.
NODE_NAMES = {
    "activity",
    "agent",
    "entity",
}


# TODO: determine what to do with "prefix"


# TODO: consider putting this in edges package.
EDGE_NAMES = {
    "wasAssociatedWith",
    "hadMember",
    "used",
    "wasInfluencedBy",
    "wasGeneratedBy"
}

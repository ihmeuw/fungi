""" Provenance-in-Elasticsearch query functions. """

import json

import logging

from esprov import ATTRIBUTE_NAME_BY_FIELD_NAME

__author__ = "Vince Reuter"
__modified__ = "2016-11-10"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.graph.nodes"


__all__ = ["Activity", "Agent", "Entity", "Record"]


_LOGGER = logging.getLogger(__modname__)



class Record(object):
    """ ADT for individual record; 1:1 correspondence with JSON/dict. """

    def __init__(self, record_data):
        """
        Create the record instance from a key-value mapping

        :param collection.abc.Mapping record_data: mapping between creator-/
            sender-side record attribute field name and field value
        """
        mapped_fields = {}
        unmapped_fields = {}
        for field, value in record_data.items():
            try:
                attr_name = ATTRIBUTE_NAME_BY_FIELD_NAME[field]
            except KeyError:
                unmapped_fields[field] = value
            else:
                mapped_fields[attr_name] = value
        self.__dict__.update(mapped_fields)


    def __eq__(self, other):
        """
        Determine whether another record is equivalent to this one.

        :param esprov.graph.nodes.Record other: record to compare with this one
        :return bool: flag indicating whether another record is
            equivalent to this one
        """
        return self.__dict__ == other.__dict__


    def __ne__(self, other):
        """
        Determine whether another record is not equivalent to this one.

        :param esprov.graph.nodes.Record other: record to compare with this one
        :return bool: flag indicating whether another record is
            not equivalent to this one
        """
        return not self == other


    def __hash__(self):
        """ Since we're overriding the equality comparison operators as we're
        interested in logical equivalence of dictionary representations each
        operand in the comparison, we should redefine hash function, too. """
        return json.dumps(self.__dict__)



class Node():
    """ General representation of a noun in the provenance universe. """
    # TODO: consider making this an ABC.
    pass



class Activity(object):
    """ Representation of an activity in the provenance model. """
    pass



class Agent(object):
    """ Representation of an agent in the provenance model. """

    def __init__(self, fullname, homedir):
        """
        Full name and home directory define an agent (Person).

        :param str fullname: user's full name
        :param str homedir: path to user's home directory
        """
        self.fullname = fullname
        self.homedir = homedir



class Entity(object):
    """ General class for nouns/entities other than activity or agent. """

    # TODO: consider deriving File and Database from this (perhaps abstract?).

    def __init__(self):
        pass


""" Provenance-in-Elasticsearch query functions. """

import logging

from elasticsearch_dsl import Search

from esprov import DOCTYPE_KEY, DOCUMENT_TYPENAMES

__author__ = "Vince Reuter"
__modified__ = "2016-11-10"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.graph.nodes"


__all__ = ["Activity", "Agent", "Entity"]



class Node():
    """ General representation of a noun in the provenance universe. """
    # TODO: consider making this an ABC.
    pass



class Activity(object):
    """ Representation of an activity in the provenance model. """



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


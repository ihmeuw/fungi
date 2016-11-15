""" Provenance-in-Elasticsearch query functions. """

import logging

from elasticsearch_dsl import Search

from esprov import DOCTYPE_KEY, DOCUMENT_TYPENAMES

__author__ = "Vince Reuter"
__modified__ = "2016-11-10"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.esprov.graph.edges"


__all__ = ["WasAssociatedWith", "Used", "WasInfluencedBy", "WasGeneratedBy"]




""" Tests for direct function to list time-lagged computational stages. """

import argparse

from elasticsearch import Elasticsearch

from esprov import HOST, PORT
from esprov.functions import list_stages

__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.functions.test_list_stages"


CLIENT = Elasticsearch(hosts=[{"host": HOST, "port": PORT}])


class TestListStages:
    """ Tests for listing recent computational stages with ES records. """


    def test_no_filters(self):
        pass


    def test_single_period_lag(self):
        pass


    def test_mixed_period_lag(self):
        pass


    def test_limit_results(self):
        pass


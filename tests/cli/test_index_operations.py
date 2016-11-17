""" Tests of basic CLI interaction with ES w.r.t. Index operations. """

"""
The basic Index operations that Elasticseach provides are creation, deletion,
existence check, and request for metadata-like information (i.e.,
settings, mappings, and aliases); these tests validate that functionality
for our CLI and also serve as a basic validation of our communication with ES.
"""

import os
import subprocess

import bin
from conftest import make_index_name


__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_index_operations"


ESPROV_PATH = os.path.join(os.path.dirname(bin.__file__), "esprov")


class TestIndexCreation:
    """ Tests for creation of an Elasticsearch Index. """


    def test_create_single_index(self, client):

        subprocess.call([ESPROV_PATH, "index", "create",
                         make_index_name("simpletest")])
        assert 1 == len(client.indices.get_alias())


    def test_create_multiple_indices(self):
        pass



class TestDeleteIndex:
    """ Tests for deletion of an Elasticsearch Index. """
    pass



class TestIndexExistence:
    """ Tests for test of existence of an Elasticsearch Index. """
    pass



class TestIndexFetch:
    """ Tests for fetch of information about an Elasticsearch Index. """
    pass

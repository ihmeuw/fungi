""" Esprov test-related utilities. """

__author__ = "Vince Reuter"
__modified__ = "2016-11-16"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.helpers.utils"


TEST_INDEX_PREFIX = "esprov-test"


def make_index_name(suffix):
    """
    Create index name for ES unit test(s) with fixed prefix and given suffix.

    :param str | int suffix: suffix for ES index to create
    :return str: index name for ES unit test(s),
        with fixed prefix and given suffix
    """
    return "{}-{}".format(TEST_INDEX_PREFIX, suffix)


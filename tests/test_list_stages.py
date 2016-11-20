""" Tests for CLI use of function to list time-lagged computational stages. """

from conftest import code_stage_text, make_index_name

__author__ = "Vince Reuter"
__modified__ = "2016-11-19"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_list_stages"

TEST_INDEX_NAME_SUFFICES = ["ti0", "t1", "ti1"]
INDEX_NAMES = [make_index_name(name) for name in TEST_INDEX_NAME_SUFFICES]
LAG_VALUES = ["-1M", "-2w", "-3d", "-12H", "-30m"]



class TestListStagesBasic:
    """ Tests for listing recent computational stages with ES records. """


    @staticmethod
    def assert_stages(expected, observed, output_format,
                      raw_to_cli=lambda x: x):
        """
        Assert equality between expected and observed results, transforming
        expectation s

        :param collections.abc.Iterable expected: collection of
            expected results
        :param collections.abc.Iterable observed: collection of
            observed results
        :param str output_format: name for way in which call was made to
            get observed output, implying the expected format
        :param function raw_to_cli: transformation from raw CLI function
            return value to format output to command line
        :return:
        """


    def test_no_data(self, es_client, observed_format):
        """ Regardless of filter(s) and index(es), no data --> no results. """

        # No Index defined at all.


        # Index, but no data.
        assert 


    def test_data_no_code_stages(self):
        """ No code stages --> no results. """
        pass


    def test_no_filters(self):
        """ Without filtering, ALL known stage names should be returned. """
        pass


    def test_single_period_lag(self):
        """ The most basic time-lag comp. stages query is single-span. """
        pass


    def test_multi_period_lag(self):
        """ Various lag spans can be chained together. """
        pass


    def test_multi_period_lag_span_collision(self):
        """ Multiple values for the same lag time span can't be given. """
        pass


    def test_specific_index(self):
        """ Specific index within which to search may be provided. """
        pass


    def test_limit_results(self):
        """ Full results may be too verbose; they can be limited. """
        pass


    def test_limit_results_single_period_lag(self):
        pass


    def test_limit_results_multi_period_lag(self):
        pass


    def test_specific_index_single_period_lag(self):
        pass


    def test_specific_index_multi_period_lag(self):
        pass


    def test_limit_results_specific_index_no_lag(self):
        pass


    def test_limit_results_specific_index_single_lag(self):
        pass


    def test_limit_results_specific_index_multi_lag(self):
        pass



class TestListStagesCustom:
    """ Tests for nice-to-have sort of features for stage run query. """
    pass

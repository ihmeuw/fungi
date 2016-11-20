""" Tests for CLI use of function to list time-lagged computational stages. """

import abc

from conftest import \
    call_cli_func, code_stage_text, make_index_name, \
    RawAndCliValidator, \
    CLI_FORMAT_NAME, RAW_FORMAT_NAME

__author__ = "Vince Reuter"
__modified__ = "2016-11-19"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_list_stages"

TEST_INDEX_NAME_SUFFICES = ["ti0", "t1", "ti1"]
INDEX_NAMES = [make_index_name(name) for name in TEST_INDEX_NAME_SUFFICES]
LAG_VALUES = ["-1M", "-2w", "-3d", "-12H", "-30m"]



class TestListStagesBasic(RawAndCliValidator):
    """ Tests for listing recent computational stages with ES records. """


    def format_output(self, results):
        """
        Format

        :param collections.abc.Iterable results: collection of
            results to reformat
        :return str: newline-joined results, such that each stage name
            would be printed on a separate line, with trailing newline
            to reset the command-line prompt
        """
        return "{}\n".format("\n".join(results))


    def test_raw_client(self, es_client, output_format):
        """ No Index --> no results. """
        expected = set()
        observed = call_cli_func("list_stages", client=es_client)
        super(TestListStagesBasic, self).validate(expected, set(observed),
                                                  output_format)


    def test_no_data(self, es_client,
                     inserted_index_and_response, output_format):
        """ Regardless of filter(s), no data --> no results. """

        # Insert and validate Index.
        index, response = inserted_index_and_response
        assert index in es_client.indices.get_alias().keys()

        expected = set()
        observed = call_cli_func("list_stages", client=es_client)
        super(TestListStagesBasic, self).validate(expected, set(observed),
                                                  output_format)


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

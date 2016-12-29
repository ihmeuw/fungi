""" Tests for CLI use of function to list time-lagged computational stages. """

import datetime
import json
import elasticsearch_dsl

import pytest

from conftest import \
    call_cli_func, code_stage_text, make_index_name, \
    parse_records_text, upload_records, RawAndCliValidator
from esprov import DOCTYPE_KEY
from esprov.dates_times import DT
from esprov.provda_record import ProvdaRecord
from .data import *

__author__ = "Vince Reuter"
__modified__ = "2016-11-21"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.tests.test_list_stages"

TEST_INDEX_NAME_SUFFICES = ["ti0", "t1", "ti1"]
INDEX_NAMES = [make_index_name(name) for name in TEST_INDEX_NAME_SUFFICES]
LAG_VALUES = ["-1M", "-2w", "-3d", "-12H", "-30m"]



class TestListStagesBasic(RawAndCliValidator):
    """ Tests for listing recent computational stages with ES records. """


    @staticmethod
    def time_diff(dt_text):
        """


        :param str dt_text: datetime-encoding text
        :return :
        """
        # TODO: resume here; implement & docstring.
        then = DT.parse(dt_text)
        delta = datetime.datetime.now() - then


    @pytest.fixture(scope="function", params=[False, True])
    def id_only(self, request):
        """ For requesting test case, use just ID or use full record. """
        return request.param


    @staticmethod
    def insert_id_in_command(command, just_id):
        """
        If needed, add id flag to command.

        :param str command: initial command
        :param bool just_id: flag indicating whether ID flag should be added
        :return str: completed command
        """
        return "{}{}".format(command, " --id" if just_id else "")


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
        observed = call_cli_func("list_stages", client=es_client)
        self.validate(set(), set(observed), output_format)


    def test_no_data(self, es_client, output_format,
                     inserted_index_and_response):
        """ Regardless of filter(s), no data --> no results. """

        # Insert and validate Index.
        index, response = inserted_index_and_response
        assert index in es_client.indices.get_alias().keys()

        observed = call_cli_func("list_stages", client=es_client)
        self.validate(expected=set(), observed=set(observed),
                      output_format=output_format)


    def test_data_no_code_stages(self, es_client, output_format, id_only):
        """ No code stages --> no results. """

        id_insertion_kwargs = {"command": "list_stages", "just_id": id_only}

        # TODO: currently, some ambiguity around "code stage."
        # TODO (continued): That is, "activity" vs. "entity" with "instance"
        # TODO (continued): mapped to "code:<script_name>"; once better
        # TODO (continued): defined, this test (+ others?) could be updated.

        # Single-index
        index1 = make_index_name("index1")
        ProvdaRecord.init(index=index1, using=es_client)
        upload_records(client=es_client,
                       records_by_index=NON_ENTITY_NON_ACTIVITY_LOGS,
                       index_name=index1)

        observed = call_cli_func(
                self.insert_id_in_command(**id_insertion_kwargs),
                client=es_client
        )
        self.validate(expected=set(), observed=set(observed),
                      output_format=output_format)

        # Multi-index
        index2 = make_index_name("index2")
        ProvdaRecord.init(index=index2, using=es_client)
        upload_records(client=es_client,
                       records_by_index=NON_ENTITY_NON_ACTIVITY_LOGS,
                       index_name=index2)

        observed = call_cli_func(
                self.insert_id_in_command(**id_insertion_kwargs),
                client=es_client
        )
        self.validate(expected=set(), observed=set(observed),
                      output_format=output_format)


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

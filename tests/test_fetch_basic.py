""" Tests for the basic record-fetching functionality """

import itertools

import pytest

from bin import cli
from .conftest import call_cli_func, make_index_name, upload_records
from .data import *
from esprov import functions, DOCTYPE_KEY, DOCUMENT_TYPENAMES


__author__ = "Vince Reuter"
__modified__ = "2016-11-09"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "provda_sandbox.tests.functions.test_fetch_basic"



class TestBasicFetch:
    """ Tests for basic provenance record fetching functionality """
    # Note that in some of these situations, due to lazy generator evaluation,
    # we need to have a dummy call like list() in order to force evaluation.
    # This is needed even to make an assertion about exception raising.
    # TODO: test on-the-fly capability.
    # TODO: note that fetch() supports -i/--index, -n/--num_docs, and --doctype.
    # TODO: test invalid arguments.
    # Note how null arguments are covered by cases where CLI call omits option.
    # TODO: presence table:
    # index    num_docs    doctype
    # F        F           F
    # F        F           T
    # F        T           F
    # F        T           T
    # T        F           F
    # T        F           T
    # T        T           F
    # T        T           T

    # TODO: the basic pattern/idiom for a functional test should be
    # TODO (continued): (as is in test_list_stages.py), as follows:
    # 1 -- make_index_name(name)
    # 2 -- ProvdaRecord.init(index, using=es_client)
    # 3 -- upload_records(client, records_by_index, index_name)
    # 4 -- get observation(s) with call_cli_func()
    # 5 -- validation(s) / assertion(s)


    # Group the option names by type of argument expected.
    # Note also the potential to partition on time-of-application, i.e.
    # pre-result (matching) vs. post-result (filtering).
    TEXTUAL_OPTIONS = ["index", "doctype"]
    NUMERIC_OPTIONS = ["num_docs"]

    # Also provide a base all-options collection.
    ALL_OPTIONS = TEXTUAL_OPTIONS + NUMERIC_OPTIONS


    @pytest.mark.parametrize(argnames="option_name", argvalues=ALL_OPTIONS)
    def test_doctype_as_flag_not_option(self, option_name, es_client):
        """ Argument-expecting option as flag --> parse error --> sys exit. """
        command = "fetch --{}".format(option_name)
        with pytest.raises(SystemExit):
            # Here, exception is in parsing, so we need no evaluation forcing.
            call_cli_func(command, client=es_client)


    @pytest.mark.parametrize(
            argnames="option,argument",
            argvalues=[("index", "unknown_index"),
                       ("doctype", "unknown_doctype")]
    )
    # TODO: choose parametrization strategy here.
    def test_invalid_option_argument(self, option, argument, es_client):
        """ Invalid argument for fetch option --> exception with func call. """
        command = "fetch --{o} {a}".format(o=option, a=argument)
        with pytest.raises(Exception):
            list(call_cli_func(command, client=es_client))


    def test_num_docs_zero(self, es_client):
        """ Zero as result count is valid; the result should just be empty. """
        # This is analogous to invalid test cases for text arguments.
        # Here, though, we get an empty result rather than an exception.
        assert [] == list(call_cli_func("fetch --num_docs 0",
                                        client=es_client))


    @pytest.mark.parametrize(argnames="num_docs", argvalues=[-5, -1])
    def test_num_docs_negative(self, num_docs, es_client):
        """ Negative count is questionable; let's allow & return empty. """
        # This is analogous to invalid test cases for text arguments.
        # Here, though, we get an empty result rather than an exception.
        command = "fetch --num_docs {}".format(num_docs)
        assert [] == list(call_cli_func(command, client=es_client))


    @pytest.mark.parametrize(
            argnames="option_and_argument",
            argvalues=itertools.product(ALL_OPTIONS, ["", " ", "  "]))
    def test_invalid_whitespace_option_argument(
            self, invalid_doctype, es_client
    ):
        """ Empty/whitespace doctype causes ValueError in document fetcher. """
        # Do the parsing without any options.
        args = cli.CLIFactory.get_parser().parse_args(["fetch"])
        # Manually set target option to invalid argument within args namespace.
        setattr(args, "doctype", invalid_doctype)
        with pytest.raises(ValueError):
            list(functions.fetch(es_client, args))


    def test_no_arguments(self, es_client):
        """ When no doctype is specified, all documents match. """
        # This is essentially a smoketest since there are no arguments.
        # The only real difference here is the check for concordance between
        # expected results count and observed results count.
        upload_records(es_client, ALL_LOGS)
        expected_results_count = len(ALL_LOGS)
        observed_results_count = \
                len(list(call_cli_func("fetch", client=es_client)))
        assert expected_results_count == observed_results_count


    def test_just_index(self, es_client):
        """ With specific index, only docs from it are returned. """
        # TODO: partition records by index

        command_template = "fetch --index {}"

        # Create an initial index.
        index1_suffix = "index1"
        index1_name = make_index_name(index1_suffix)
        index1_records = ACTIVITY_LOGS
        # Insert the activity-entity-only collection of log records.
        upload_records(client=es_client,
                       records_by_index=index1_records,
                       index_name=index1_name)
        # Get results and compare to expectation.
        command = command_template.format(index1_name)
        results_after_one_index = list(call_cli_func(command,
                                                     client=es_client))
        assert index1_records == results_after_one_index

        # Create second index and insert code-specific logs.
        index2_suffix = "index2"
        index2_records = CODE_LOGS
        index2_name = make_index_name(index2_suffix)
        upload_records(client=es_client,
                       records_by_index=index2_records,
                       index_name=index2_name)
        # Check each set of records is accessible but that they're disjoint.
        command = command_template.format(index2_name)
        result_second_after_both_indices = \
                list(call_cli_func(command, client=es_client))
        assert index2_records == result_second_after_both_indices
        command = command_template.format(index1_name)
        results_first_after_both_indices = \
                list(call_cli_func(command, client=es_client))
        assert index1_records == results_first_after_both_indices


    @pytest.mark.parametrize(argnames="known_doctype",
                             argvalues=DOCUMENT_TYPENAMES)
    def test_just_doctype(self, known_doctype, es_client):
        """ With specific doctype, only matched docs are returned. """

        command = "fetch --doctype{}".format(" {}".format(known_doctype))
        results = list(call_cli_func(command, client=es_client))
        violators = filter(
            lambda record: record[DOCTYPE_KEY] == known_doctype, results
        )

        # Fail the test if there's even a single violator.
        if len(violators) > 0:
            raise AssertionError(
                "{} result(s) don't match expected doctype {}: {}".
                    format(len(violators), known_doctype, "\n".join(violators))
            )
        # No violators --> test passes.


    @pytest.mark.parametrize(
            argnames="num_docs,doctype",
            argvalues=itertools.product([2, 5], DOCUMENT_TYPENAMES)
    )
    def test_just_num_docs(self, num_docs, known_doctype, es_client):
        """ Ensure that the results count can be controlled via CLI. """
        command = "fetch --doctype {} --num_docs {}".format(known_doctype,
                                                            num_docs)
        results = call_cli_func(command, client=es_client)
        assert num_docs == len(list(results))


    @pytest.mark.parametrize(
            argnames="option_and_argument",
            argvalues=itertools.product()
    )
    def test_index_and_doctype(self):
        """ Test specificity of command with explicit index and doctype. """
        pass


    def test_index_and_num_docs(self):
        """ Test simultaneous  """
        pass


    def test_doctype_and_num_docs(self):
        """  """
        pass


    def test_index_doctype_and_num_docs(self):
        """  """
        pass

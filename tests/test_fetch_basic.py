""" Tests for the basic record-fetching functionality """

import itertools

import pytest

from bin import cli
from .conftest import call_cli_func, make_index_name, upload_records
from .data import *
from esprov import functions, DOCTYPE_KEY, DOCUMENT_TYPENAMES
from esprov.utilities import INVALID_ITEMS_LIMIT_EXCEPTION


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
    # F        F           F        DON'T TEST, as we want to use test index.
    # F        F           T        DON'T TEST, as we want to use test index.
    # F        T           F        DON'T TEST, as we want to use test index.
    # F        T           T        DON'T TEST, as we want to use test index.
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


    @pytest.mark.parametrize(
            argnames="option_and_argument",
            argvalues=itertools.product(ALL_OPTIONS, ["", " ", "  "]))
    def test_invalid_whitespace_option_argument(
            self, option_and_argument, es_client
    ):
        """ Empty/whitespace doctype causes ValueError in document fetcher. """
        # Do the parsing without any options.
        args = cli.CLIFactory.get_parser().parse_args(["fetch"])
        # Manually set target option to invalid argument within args namespace.
        option, argument = option_and_argument
        setattr(args, option, argument)
        with pytest.raises(ValueError):
            list(functions.fetch(es_client, args))


    @pytest.mark.parametrize(argnames="num_docs", argvalues=[-5, -1, 0])
    def test_num_docs_corner(self, num_docs, es_client):
        """ Negative count is questionable; let's allow & return empty. """
        # This is analogous to invalid test cases for text arguments.
        # Here, though, we get an empty result rather than an exception.
        command = "fetch --num_docs {}".format(num_docs)
        with pytest.raises(INVALID_ITEMS_LIMIT_EXCEPTION):
            list(call_cli_func(command, client=es_client))


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


    @pytest.mark.skip("Limit verbosity during debugging")
    @pytest.mark.parametrize(argnames="known_doctype",
                             argvalues=DOCUMENT_TYPENAMES)
    def test_doctype(self, known_doctype, es_client):
        """ With specific doctype, only matched docs are returned. """

        command = "fetch --doctype{}".format(" {}".format(known_doctype))
        results = list(call_cli_func(command, client=es_client))
        violators = filter(
                lambda record: record[DOCTYPE_KEY] == known_doctype,
                results
        )

        # Fail the test if there's even a single violator.
        assert len(violators) == 0, \
                "{} result(s) don't match expected doctype {}: {}".\
                format(len(violators), known_doctype,
                       "\n".join([str(violator) for violator in violators]))
        # No violators --> test passes.


    @pytest.mark.parametrize(
            argnames="count_and_records",
            argvalues=zip(
                    [1, 2, 3, 4],
                    [ACTIVITY_LOGS, CODE_LOGS,
                     DOC_LOGS, NON_ENTITY_NON_ACTIVITY_LOGS]
            )
    )
    def test_num_docs(self, count_and_records, es_client):
        """ Ensure that the results count can be controlled via CLI. """

        # Check that we have more records than the count to which we'll filter.
        # That is, ensure that making assertion about filtration has meaning.
        num_docs, records = count_and_records
        assert num_docs < len(records)
        upload_records(client=es_client, records_by_index=records)

        # Make the assertion once results are obtained from command call.
        command = "fetch --num_docs {}".format(num_docs)
        results = call_cli_func(command, client=es_client)
        assert num_docs == len(list(results))


    @pytest.mark.skip("Unimplemented")
    @pytest.mark.parametrize(
        argnames="num_docs,doctype",
        argvalues=itertools.product([3, 4], DOCUMENT_TYPENAMES)
    )
    def test_doctype_and_num_docs(self, num_docs, doctype):
        """ Ensure that doctype and docs count can be used in tandem. """
        command = "fetch --doctype {} --num_docs {}".format(doctype,
                                                            num_docs)

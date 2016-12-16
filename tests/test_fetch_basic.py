""" Tests for the basic record-fetching functionality """

import itertools

import pytest

from bin import cli
from conftest import call_cli_func, ES_CLIENT
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


    # Group the option names by type of argument expected.
    # Note also the potential to partition on time-of-application, i.e.
    # pre-result (matching) vs. post-result (filtering).
    TEXTUAL_OPTIONS = ["index", "doctype"]
    NUMERIC_OPTIONS = ["num_docs"]

    # Also provide a base all-options collection.
    ALL_OPTIONS = TEXTUAL_OPTIONS + NUMERIC_OPTIONS


    @pytest.mark.parametrize(argnames="option_name", argvalues=ALL_OPTIONS)
    def test_doctype_as_flag_not_option(self, option_name):
        """ Argument-expecting option as flag --> parse error --> sys exit. """
        command = "fetch --{}".format(option_name)
        with pytest.raises(SystemExit):
            # Here, exception is in parsing, so we need no evaluation forcing.
            call_cli_func(command)


    @pytest.mark.parametrize(
            argnames="option,argument",
            argvalues=[("index", "unknown_index"),
                       ("doctype", "unknown_doctype")]
    )
    # TODO: choose parametrization strategy here.
    def test_invalid_option_argument(self, option, argument):
        """ Invalid argument for fetch option --> exception with func call. """
        command = "fetch --{o} {a}".format(o=option, a=argument)
        with pytest.raises(Exception):
            list(call_cli_func(command))


    # This is analogous to invalid test cases for text arguments.
    # Here, though, we get an empty result rather than an exception.
    def test_num_docs_zero(self):
        """ Zero as result count is valid; the result should just be empty. """
        assert [] == list(call_cli_func("fetch --num_docs 0"))


    # This is analogous to invalid test cases for text arguments.
    # Here, though, we get an empty result rather than an exception.
    @pytest.mark.parametrize(argnames="num_docs", argvalues=[-5, -1])
    def test_num_docs_negative(self, num_docs):
        """ Negative count is questionable; let's allow & return empty. """
        command = "fetch --num_docs {}".format(num_docs)
        assert [] == list(call_cli_func(command))


    def test_null_option_argument(self):
        """ Null argument --> absence of option from CLI call --> valid. """
        pass


    def test_no_arguments(self):
        """ When no doctype is specified, all documents match. """
        pass



    @pytest.mark.parametrize(argnames="invalid_doctype",
                             argvalues=["", " ", "  "])
    def test_invalid_empty_doctypes(self, invalid_doctype):
        """ Empty/whitespace doctype causes ValueError in document fetcher. """
        args = cli.CLIFactory.get_parser().parse_args(["fetch"])
        setattr(args, "doctype", invalid_doctype)
        with pytest.raises(ValueError):
            list(functions.fetch(ES_CLIENT, args))


    def test_invalid_nonempty_doctype(self):
        """ Invalid doctype causes a ValueError in document fetcher. """
        command = "fetch --doctype invalid_doctype"
        with pytest.raises(ValueError):
            list(call_cli_func(command))


    @pytest.mark.parametrize(argnames="known_doctype",
                             argvalues=DOCUMENT_TYPENAMES)
    def test_known_doctype(self, known_doctype):
        """ With specific doctype, only matched docs are returned. """

        command = "fetch --doctype{}".format(" {}".format(known_doctype))
        results = list(call_cli_func(command))
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
        argnames="known_index,known_doctype",
        argvalues=
    )
    # TODO: determine parametrization strategy.

    def test_known_index(self, known_doctype, known_index):
        """ With specific index, only docs from it are returned. """
        # TODO: parametrize with known index names here.
        pass


    @pytest.mark.parametrize()
    def test_unknown_index(self, index):
        command = "fetch --index {}".format(index)
        results = call_cli_func(command)


    @pytest.mark.parametrize(
            argnames="num_docs,doctype",
            argvalues=itertools.product([2, 5], DOCUMENT_TYPENAMES)
    )
    def test_num_docs(self, num_docs, known_doctype):
        """ Ensure that the results count can be controlled via CLI. """
        command = "fetch --doctype {} --num_docs {}".format(known_doctype,
                                                            num_docs)
        results = call_cli_func(command)
        assert num_docs == len(list(results))

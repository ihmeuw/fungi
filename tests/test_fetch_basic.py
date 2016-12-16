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

    DOCTYPE_CMD_BASE = "fetch --doctype{}"


    def test_missing_doctype(self):
        with pytest.raises(SystemExit):
            # Here, exception is in parsing, so we need no evaluation forcing.
            call_cli_func(command=self.DOCTYPE_CMD_BASE.format(""))


    @pytest.mark.parametrize(argnames="invalid_doctype",
                             argvalues=["", " ", "  ", ])
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

        command = self.DOCTYPE_CMD_BASE.format(" {}".format(known_doctype))
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


    @pytest.mark.parametrize()
    def test_specific_index(self, known_doctype):
        """ With specific index, only docs from it are returned. """
        pass


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

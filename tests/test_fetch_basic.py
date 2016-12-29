""" Tests for the basic record-fetching functionality """

import copy
from functools import partial
import itertools
import logging
import operator
import subprocess

from elasticsearch_dsl import Search
import pytest

from bin import cli
from .conftest import \
    call_cli_func, equal_sans_time, \
    make_index_name, upload_records
from .data import *
from esprov import functions, DOCTYPE_KEY, DOCUMENT_TYPENAMES, TIMESTAMP_KEY
from esprov.utilities import IllegalItemsLimitException


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


    @pytest.mark.skip("Isolate index name test")
    @pytest.mark.parametrize(argnames="option_name", argvalues=ALL_OPTIONS)
    def test_doctype_as_flag_not_option(self, option_name, es_client):
        """ Argument-expecting option as flag --> parse error --> sys exit. """
        command = "fetch --{}".format(option_name)
        with pytest.raises(SystemExit):
            # Here, exception is in parsing, so we need no evaluation forcing.
            call_cli_func(command, client=es_client)


    @pytest.mark.skip("Isolate index name test")
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


    @pytest.mark.skip("Isolate index name test")
    @pytest.mark.parametrize(
            argnames="option_and_argument",
            argvalues=itertools.product(["doctype", "num_docs"],
                                        ["", " ", "  "])
    )
    def test_invalid_whitespace_option_argument(
            self, option_and_argument, es_client
    ):
        """ Empty/whitespace doctype/num_docs causes ValueError in fetcher. """
        # Do the parsing without any options.
        args = cli.CLIFactory.get_parser().parse_args(["fetch"])
        # Manually set target option to invalid argument within args namespace.
        option, argument = option_and_argument
        setattr(args, option, argument)
        with pytest.raises(ValueError):
            list(functions.fetch(es_client, args))


    @pytest.mark.skip("Isolate index name test")
    @pytest.mark.parametrize(
            argnames="index_argument",
            argvalues=["", " ", "  "]
    )
    def test_whitespace_index(self, index_argument, es_client):
        """ Whitespace argument for index option results in no match. """
        args = cli.CLIFactory.get_parser().parse_args(["fetch"])
        setattr(args, "index", index_argument)
        assert index_argument not in es_client.indices.get_alias()
        # DEBUG
        print "INDEX: {} ({})".format(index_argument, len(index_argument))
        with pytest.raises(ValueError):
            list(functions.fetch(es_client, args))


    @pytest.mark.skip("Isolate index name test")
    @pytest.mark.parametrize(argnames="num_docs", argvalues=[-5, -1, 0])
    def test_num_docs_corner(self, num_docs, es_client):
        """ Negative count is questionable; let's allow & return empty. """
        # This is analogous to invalid test cases for text arguments.
        # Here, though, we get an empty result rather than an exception.
        command = "fetch --num_docs {}".format(num_docs)
        with pytest.raises(IllegalItemsLimitException):
            list(call_cli_func(command, client=es_client))


    def test_just_index(self, es_client):
        """ With specific index, only docs from it are returned. """
        # TODO: partition records by index

        command_template = "fetch --index {}"

        # Create an initial index.
        index1_suffix = "index1"
        index1_name = make_index_name(index1_suffix)
        command = command_template.format(index1_name)

        # Immediate assertion to ensure target test index doesn't exist.
        with pytest.raises(ValueError):
            list(call_cli_func(command, client=es_client))

        index1_records = ACTIVITY_LOGS[:2]
        # Insert the activity-entity-only collection of log records.
        logging.info("Uploading %d records to index '%s'",
                     len(index1_records), index1_name)
        upload_records(client=es_client,
                       records_by_index=index1_records,
                       index_name=index1_name)

        # Mostly DEBUG, but some additional ordinary validation, too.
        try:
            assert len(index1_records) == \
                   sum(1 for _ in Search(index=index1_name))
        except AssertionError:
            debugging_command = \
                "curl -XGET 'localhost:9200/_search?pretty' -d'{\"query\": {\"match_all\": {}}}'"
            with open("debugging.txt", "w") as f:
                subprocess.check_call(
                    debugging_command.split(" "), stdout=f, stderr=f
                )
            raise

        # Get results and compare to expectation.
        results_after_one_index = \
                list(call_cli_func(command, client=es_client))
        unmatched_expectations, unused_observations = self.discrepancies(
            expected=index1_records,
            observed=results_after_one_index,
            equivalence_comparator=equal_sans_time
        )
        # DEBUG
        try:
            assert [] == unmatched_expectations
            assert [] == unused_observations
        except AssertionError:
            print("COMMAND: {}".
                  format(command))
            print("EXPECTED RECORD COUNT: {}".
                  format(len(index1_records)))
            print("OBSERVED RECORD COUNT: {}".
                  format(len(results_after_one_index)))
            print("INDICES: {}".
                  format(es_client.indices.get_alias()))
            print("{} UNMATCHED EXPECTATIONS:\n{}".format(
                len(unmatched_expectations),
                "\n".join([str(e) for e in unmatched_expectations]))
            )
            print("{} UNUSED OBSERVATIONS:\n{}".format(
                len(unused_observations),
                "\n".join([str(o) for o in unused_observations]))
            )
            raise

        # Create second index and insert code-specific logs.
        index2_suffix = "index2"
        index2_records = CODE_LOGS[:2]
        index2_name = make_index_name(index2_suffix)
        upload_records(client=es_client,
                       records_by_index=index2_records,
                       index_name=index2_name)

        # Check each set of records is accessible but that they're disjoint.
        command = command_template.format(index2_name)
        result_second_after_both_indices = \
                list(call_cli_func(command, client=es_client))
        unmatched_expectations, unused_observations = self.discrepancies(
            expected=index2_records,
            observed=result_second_after_both_indices,
            equivalence_comparator=equal_sans_time
        )
        assert [] == unmatched_expectations
        assert [] == unused_observations

        command = command_template.format(index1_name)
        results_first_after_both_indices = \
                list(call_cli_func(command, client=es_client))
        unmatched_expectations, unused_observations = self.discrepancies(
            expected=index1_records,
            observed=results_first_after_both_indices,
            equivalence_comparator=equal_sans_time
        )
        assert [] == unmatched_expectations
        assert [] == unused_observations



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


    @pytest.mark.skip("isolate failing tests")
    @pytest.mark.parametrize(
            argnames="count_and_records",
            argvalues=zip(
                    [1, 2, 3, 4, 15],
                    [ACTIVITY_LOGS, CODE_LOGS,
                     DOC_LOGS, NON_ENTITY_NON_ACTIVITY_LOGS,
                     ACTIVITY_LOGS + CODE_LOGS]
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


    @staticmethod
    def discrepancies(expected, observed, equivalence_comparator=None):
        """
        Determine any discrepancy between expected observations and actual
        observations, making an attempt to find a match for each expected
        value in the collection of observed values. Discrepancies
        (return values) are represented as a pair of lists.

        :param collections.abc.Iterable expected: collection of
            expected observations
        :param collections.abc.Iterable observed: collections of
            actual observations
        :param (object, object) -> bool equivalence_comparator: function with
            which to compare two object; optional, default to equality operator
        :return list, list: pair of lists, the first of which is a
            collection of expected observations for which no actual
            observation was a match, and the second of which is a
            collection of actual observations, each of which was not
            used as a match for one of the expected observations. If
            the input collection arguments match and there's a logical
            equivalence between expectation and observation, each of
            the returned collections should be empty.
        """

        equivalence_comparator = equivalence_comparator or operator.eq

        expected_clone = copy.deepcopy(expected)
        observed_clone = copy.deepcopy(observed)

        expected_indices_unused = []
        observed_indices_used = []

        for i, e in enumerate(expected_clone):
            for j, o in enumerate(observed_clone):
                if equivalence_comparator(e, o):
                    observed_indices_used.append(j)
                    break
            else:
                logging.debug("No match for\n%s\nin\n%s",
                              e, str(observed_clone))
                expected_indices_unused.append(i)

        unmatched_expectations = [expected_clone[i]
                                  for i in expected_indices_unused]
        unused_observations = [o for j, o in enumerate(observed_clone)
                               if j not in set(observed_indices_used)]

        return unmatched_expectations, unused_observations

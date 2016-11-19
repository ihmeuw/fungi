""" Executable for working with provenance-oriented data in Elasticsearch. """

import argparse
from collections import namedtuple

from esprov.functions import \
    fetch, list_stages, index, \
    INDEX_OPERATION_NAMES, LIST_STAGES_TIMESPANS


__author__ = "Vince Reuter"
__modified__ = "2016-11-10"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "esprov.bin.cli"


# TODO: remove notes.
# Relevant components of query that is sent to ES:
# 1 -- "Query" (JSON object)
# 2 -- "Size"
# 3 -- "Fields" (document fields to include in result) -->
# TODO: from each hit, or from result object?

# Others, unsupported here
# 1 -- "From" --> Offset into results (default 0)
# 2 -- "Facets" --> Summary information about specific field(s) in data
# 3 -- "Filter" -- > Special case to apply filter/query to results not facets


# Mechanism to define argparse argument(s) for particular subcommand.
Argument = namedtuple(
    "Argument",
    field_names=['flags', 'help', 'action', 'default',
                 'nargs', 'type', 'choices', 'metavar']
)
# Establish null default for each of the argparse Argument parameters.
Argument.__new__.__defaults__ = (None, ) * len(Argument._fields)


# TODO: write tests around the help texts, descriptions, and error conditions.


class _Subparser(object):
    """ Argument parser for specific CLI function """

    def __init__(self, function, argument_names=(), description=""):
        """
        Function, argument names, and description define a CLI subparser.

        :param callable function:
        :param collections.abc.(str) argument_names: sequence
        :param str description: subcommand functional description
        """
        # TODO: test description derivation from __doc__.
        self.function = function
        self.argument_names = argument_names
        self.description = \
            description or function.__doc__.strip().split("\n")[0]


class CLIFactory(object):
    """ Factory for CLI subcommand parser """

    # Subcommand-agnostic argument
    arguments = {

        # Basic arguments shared and valid for EVERY CLI function
        # TODO: determine if this will work given index() function.
        "index": Argument(
                flags=("-i", "--index"),
                help="Index to query",
                default="_all"
        ),
        "num_docs": Argument(
                flags=("-n", "--num_docs"),
                help="Limit for number of search hits",
                type=int
        ),

        # Arguments shared but valid only for SOME CLI functions
        "doctype": Argument(
                flags=("--doctype", ),
                help="Document type to query"
        ),
        "duplicate": Argument(
                flags=("--duplicate", ),
                help="Retain duplicates in results",
                action="store_true"
        ),

        # "list_stages" family of arguments
        # Obviously, there's more intricate logic w.r.t. time.
        # TODO: write corner case tests to ensure that there's wonky handling.
        "months": Argument(
                flags=("--months", ),
                help="Integer number of months of lag"
        ),
        "weeks": Argument(
                flags=("--weeks", ),
                help="Integer number of weeks of lag",
                type=int
        ),
        "days": Argument(
                flags=("--days", ),
                help="Integer number of days of lag",
                type=int
        ),
        "hours": Argument(
                flags=("--hours", ),
                help="Integer number of hours of lag",
                type=int
        ),
        "minutes": Argument(
                flags=("--minutes", ),
                help="Integer number of minutes of lag",
                type=int
        ),

        "index_operation": Argument(
                # When it's used, index_operation is required.
                flags=("index_operation", ),
                help="Name of Elasticsearch Index operation to perform",
                choices=list(INDEX_OPERATION_NAMES)
        ),
        "index_target": Argument(
                # Generally, index name is optional. This version is required.
                flags=("index_target", ),
                help="Name of Elasticsearch Index for Index operation"
        )

    }

    # Shared and valid for all CLI functions.
    BASE_ARGS = ("index", "num_docs")

    subparsers = (
        _Subparser(
                fetch,
                argument_names=(BASE_ARGS + ("doctype", ))
        ),
        _Subparser(
                list_stages,
                argument_names=(("duplicate", ) +
                                BASE_ARGS +
                                LIST_STAGES_TIMESPANS)
        ),
        _Subparser(
                index,
                argument_names=("index_operation", "index_target"),
        )
    )


    @classmethod
    def get_parser(cls):
        """
        Create command-line argument parser for a CLI run.

        :return argparse.ArgumentParser: parser for CLI run.
        """

        # Require subcommand to supplement basic executable name.
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(help="subcommand help",
                                           dest="subcommand")
        subparsers.required = True

        for subparser in cls.subparsers:
            # Add parser for each subcommand.
            sp = subparsers.add_parser(subparser.function.__name__,
                                       help=subparser.description)

            # Add all arguments for current subcommand parser.
            for argument_name in subparser.argument_names:
                argument = cls.arguments[argument_name]

                # Parse and bundle current argument's argparse K-V pairs.
                kwargs = {
                    # 'flags' just stores the ways in which the option
                    # may be specified via CLI; we want non-null argparse
                    # K-V pairs only, as mixing certain combinations of
                    # K-V pairs seems to be invalid (e.g., action="store_true"
                    # with type=<anything>, even with <anything>=None.)
                    field: getattr(argument, field)
                    for field in argument._fields
                    if field != 'flags' and getattr(argument, field)
                }

                # Add the argument, providing reference
                # mode(s)/name(s) and argparse K-V pairs.
                sp.add_argument(*argument.flags, **kwargs)

            # Allow invocation of subcommand via args.func(args).
            sp.set_defaults(func=subparser.function)

        return parser

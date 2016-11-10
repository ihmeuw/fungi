""" Executable for working with provenance-oriented data in Elasticsearch. """

import argparse
from collections import namedtuple

from esprov.functions import *

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


Argument = namedtuple(
    "Argument",
    field_names=['flags', 'help', 'action', 'default',
                 'nargs', 'type', 'choices', 'metavar']
)
Argument.__new__.__defaults__ = (None, ) * len(Argument._fields)


class _Subparser(object):
    """ Argument parser for specific CLI function """

    def __init__(self, function, argument_names=(), description=""):
        """
        Function, argument names, and help text define a CLI subparser.

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
        "doctype": Argument(flags=("doctype", ),
                            help="Document type to query"),
        "index": Argument(flags=("-i", "--index"),
                          help="Index to query",
                          default="_all"),
        "num_docs": Argument(flags=("-n", "--num_docs"),
                             help="Limit for number of search hits",
                             type=int)
    }

    subparsers = (
        _Subparser(fetch, argument_names=("doctype", "index", "num_docs")),
    )

    subparser_names = (sub.__name__ for sub in subparsers)


    @classmethod
    def get_parser(cls):
        """
        Create command-line argument parser for a CLI run.

        :return argparse.ArgumentParser: parser for CLI run.
        """

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(help="sub-command help",
                                           dest="subcommand")
        subparsers.required = True

        for subparser in cls.subparsers:
            sp = subparsers.add_parser(subparser.function.__name__,
                                       help=subparser.description)

            for argument_name in subparser.argument_names:
                argument = cls.arguments[argument_name]
                kwargs = {
                    field: getattr(argument, field)
                    for field in argument._fields if field != 'flags'
                }
                sp.add_argument(*argument.flags, **kwargs)

            # Allow invocation of subcommand via args.func(args).
            sp.set_defaults(func=subparser.function)

        return parser

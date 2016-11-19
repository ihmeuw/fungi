#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Installation setup script for Elasticsearch provenance package. """


from codecs import open
import os
from setuptools import setup

__author__ = "Vince Reuter"
__modified__ = "2016-11-17"
__credits__ = ["Vince Reuter"]
__maintainer__ = "Vince Reuter"
__email__ = "vr24@uw.edu"
__modname__ = "setup"


root = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(root, "README.rst"), 'r') as readme:
    full_description = readme.read()


setup(

    name="esprov",

    version="0.0.0",

    author=__author__,
    author_email=__email__,

    description="Provenance + Elasticsearch CLI",
    license="BSD",
    keywords="provenance elasticsearch",

    # TODO: register with PyPI and then un-comment-out.
    #url="http://packages.python.org/esprov",

    packages=["bin", "esprov", "tests"],

    include_package_data=True,
    zip_safe=False,
    platforms="any",
    install_requires=[
        "elasticsearch>=2.0.0,<3.0.0",
        "elasticsearch-dsl>=2.0.0,<3.0.0"
        "pytest>=3.0.4"
    ],

    long_description=full_description,

    classifiers=[
        "Development Status :: 1 - Planning"
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules"

    ]

)

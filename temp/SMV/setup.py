#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Template from: https://github.com/kennethreitz/setup.py/blob/master/setup.py

# Local test -- inside of a clean virtualenv:
# pip install .
# pip install .[pyspark]  -- to test with pyspark support

# Packing Background
#
# 1) the "name" section indicates the name of our package as referenced on pypi and is
#    referred to in the documentation as the "root package"
# 2) the MANIFEST.in file indicates what files should be uploaded to PyPi during the upload
#    upload step. They dictate the contents of the wheel and source distribution (i.e. .zip file)
#    that forms the distribution. This behavior is enabled by "include_package_data=True"
# 3) the "packages" list indicates what packages -- i.e. directories -- should be created inside
#    python's site-packages directory. Any entry with a "." indicates to create a subdirectory
#    (e.g. 'smv.target' means create a target directory inside of the site-packages/smv directory)
# 3) the "packages_data" dict ties together the MANIFEST to the "packages" section.
#    the keys of the "packages_data" dict indicate the location (i.e. directory or subdirectory)
#    to copy (i.e. install) files into. An empty section ('') indicates the "root package" as specifed
#    by the "name" section. Any name with a dot indicates a sub-directory (e.g. smv.target
#    means copy into the the subdirectory smv/target).
#    The values of the packages dict indicate which files should be copies from the MANIFEST into the
#    directory specified by the "key"

# Instructions for adding new files to the distribution:
#
# 1. If the directory is to be within an existing sub-package, then the only thing you need
#    to do is make sure the contents are refernced inside of the MANIFEST.in file, and ignore
#    the rest of these instructions
# 2. If the addition is of a NEW directory in the release "root", then declare
#    it as an smv sub-package in the "packages" section. Then add the contents you
#    want to appear in the release to the MANIFEST.in file. Finally, add an entry
#    to the package_dir section indicating which smv sub-package should receive the
#    new files added to the MANIFEST.in

import io
import os
import sys
import setuptools

here = os.path.abspath(os.path.dirname(__file__))

def read_file(path_from_root):
    with io.open(os.path.join(here, path_from_root), encoding='utf-8') as f:
        return f.read().strip()

# Package meta-data.
NAME = 'smv'
DESCRIPTION = 'A modular framework for creating applications in Apache Spark'
URL = 'https://github.com/TresAmigosSD/SMV'
EMAIL = 'bzhangusc@live.com'
AUTHOR = 'Bo Zhang, Ali Tajeldin, Kai Chen, Lane Barlow, Guangning Yu'
REQUIRES_PYTHON = '>=2.7'
VERSION = read_file('.smv_version')
README_CONTENTS = read_file('README.md')

# What packages are required for this module to be executed?
requirements_file_path = os.path.join("docker", "smv", "requirements.txt")
requirements_file_as_list = read_file(requirements_file_path).split('\n')

# What packages are optional?
# this is that powers the pip install smv[pyspark] syntax
EXTRAS = {
    'pyspark': ['pyspark'],
}

# Where the magic happens:
setuptools.setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=README_CONTENTS,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    tests_require=['sphinx', 'tox'],
    # Need to call find_packages so that newly introduced
    # sub-packages will be included
    packages=setuptools.find_packages(
        "src/main/python",
        exclude=['test_support', 'scripts']
    ) + [
        'smv.target',
        'smv.docker',
        'smv.docs',
        'smv.tools',
        'smv.src',
    ],
    # https://docs.python.org/2/distutils/setupscript.html#listing-whole-packages
    package_dir={
        '':'src/main/python',
        'smv.target': 'target/scala-2.11',
        'smv.docker': 'docker',
        'smv.docs': 'docs',
        'smv.tools': 'tools',
        'smv.src': 'src',
    },
    include_package_data=True,
    scripts=[
        'tools/smv-run',
        'tools/smv-shell',
        'tools/smv-server',
        'tools/smv-init',
        'tools/smv-jupyter',
        'tools/smv-pytest',
        'tools/spark-install',
        'tools/_env.sh',
    ],
    install_requires=requirements_file_as_list,
    extras_require=EXTRAS,
    license='Apache License, Version 2.0',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Template from: https://github.com/kennethreitz/setup.py/blob/master/setup.py

# Local test -- inside of a clean virtualenv:
# pip install .
# pip install .[pyspark]  -- to test with pyspark support

# Instructions for adding new files to the distribution:
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
from shutil import rmtree

here = os.path.abspath(os.path.dirname(__file__))

def read_file(path_from_root):
    with io.open(os.path.join(here, path_from_root), encoding='utf-8') as f:
        return f.read()

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

class UploadCommand(setuptools.Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPI via Twine…')
        os.system('twine upload dist/*')

        self.status('Pushing git tags…')
        os.system('git tag v{0}'.format(VERSION))
        os.system('git push --tags')

        sys.exit()

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
    packages=[
        'smv',
        'smv.target',
        'smv.docker',
        'smv.docs',
        'smv.tools',
        'smv.src',
    ],
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
    license='Apache',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    # $ setup.py publish support.
    cmdclass={
        'upload': UploadCommand,
    },
)

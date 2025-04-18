#!/usr/bin/env python
# Licensed under a 3-clause BSD style license - see LICENSE.rst

import glob
import os
from setuptools import find_packages

from setuptools import setup

# read the README.md file and return as string.
def readme():
    with open('README.md') as r_obj:
        return r_obj.read()

# Get some values from the setup.cfg
try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

conf = ConfigParser()
conf.optionxform=str
conf.read(['setup.cfg'])
metadata = dict(conf.items('metadata'))

PACKAGENAME = metadata.get('package_name', 'packagename')
DESCRIPTION = metadata.get('description', 'CADC package')
AUTHOR = metadata.get('author', 'CADC')
AUTHOR_EMAIL = metadata.get('author_email', 'cadc@nrc.gc.ca')
LICENSE = metadata.get('license', 'unknown')
URL = metadata.get('url', 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca')

# VERSION should be PEP386 compatible (http://www.python.org/dev/peps/pep-0386)
VERSION = metadata.get('version', 'none')

# generate the version file
with open(os.path.join(PACKAGENAME, 'version.py'), 'w') as f:
    f.write('version = \'{}\'\n'.format(VERSION))

# Treat everything in scripts except README.md as a script to be installed
scripts = [fname for fname in glob.glob(os.path.join('scripts', '*'))
           if os.path.basename(fname) != 'README.md']

# Define entry points for command-line scripts
entry_points = {'console_scripts': []}

entry_point_list = conf.items('entry_points')
for entry_point in entry_point_list:
    entry_points['console_scripts'].append('{0} = {1}'.format(entry_point[0],
                                                              entry_point[1]))

install_requires=metadata.get('install_requires', '').strip().split()

setup(name=PACKAGENAME,
      version=VERSION,
      description=DESCRIPTION,
      scripts=scripts,
      install_requires=install_requires,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      url=URL,
      long_description=readme(),
      zip_safe=False,
      use_2to3=False,
      setup_requires=['pytest-runner'],
      entry_points=entry_points,
      packages=find_packages(),
      package_data={PACKAGENAME: ['data/*', 'tests/data/*', '*/data/*', '*/tests/data/*']},
      classifiers=[
        'Natural Language :: English',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6'
      ],
)

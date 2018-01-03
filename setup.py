# -*- coding: utf-8 -*-

"""
Copyright (C) 2014-2018 OceanDataLab

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import os
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand
try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess


class Tox(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_siote = True

    def run_tests(self):
        import tox
        errcode = tox.cmdline(self.test_args)
        sys.exit(errcode)


package_dir = os.path.dirname(__file__)
version_path = os.path.join(package_dir, 'VERSION.txt')

major_version = '0.1'
if os.path.exists('.git') and os.path.isdir('.git'):
    gitrev = ['/usr/bin/git', 'rev-list', 'HEAD', '--count']
    commits = subprocess.check_output(gitrev).decode('utf-8').strip()
    with open(version_path, 'w') as f:
        f.write('{}.{}'.format(major_version, commits))

with open(version_path, 'r') as f:
    version = f.read()

setup(
    zip_safe=False,
    name='syntool_exporter',
    version=version,
    author='Sylvain Herlédan <sylvain.herledan@oceandatalab.com>',
    author_email='syntool@oceandatalab.com',
    url='https://git.oceandatalab.com/syntool_odl/exporter',
    packages=['syntool_exporter'],
    scripts=[],
    license='AGPLv3',
    description='Syntool SQL export tool.',
    long_description=open('README.txt').read(),
    install_requires=['sqlalchemy'],
    tests_require=['tox', 'virtualenv'],
    cmdclass={'test': Tox},
    package_data={'syntool_exporter': ['share/*.*']},
    entry_points={'console_scripts': [
        'syntool-meta2sql = syntool_exporter.cmd:to_sql']}
)

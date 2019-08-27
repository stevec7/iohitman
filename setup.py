#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(name='iohitman',
      version='3.0',
      description='Parallel I/O tester',
      author='stevec7',
      packages=['iohitman', 'iohitman.testsuite'],
      package_dir = {
        'iohitman': 'iohitman',
        'iohitman.testsuite': 'iohitman/testsuite',
      }
      scripts = [
        'contrib/bin/run_iohitman.py',
      ],
    )

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

requirements = [
    'pandas',
    'PyYAML',
    'pyodbc',
    'snowflake-connector-python',
]
setup_requirements = [
    'pytest-runner',
]

test_requirements = [
    'pytest',
    'mock',
]

setup(
    author="Samuel Luen-English",
    author_email='sam.luenenglish@atheon.co.uk',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="DBio, an abstraction around various database technologies.",
    install_requires=requirements,
    include_package_data=True,
    keywords='dbio',
    name='dbio',
    packages=find_packages(include=['dbio']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    version='3.0.1',
)

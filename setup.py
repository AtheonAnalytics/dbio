#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [ ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Samuel Luen-English",
    author_email='sam.luenenglish@atheon.co.uk',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="DBio, an abstraction around various database technologies.",
    install_requires=requirements,
    long_description=readme,
    include_package_data=True,
    keywords='dbio',
    name='dbio',
    packages=find_packages(include=['dbio']),
    setup_requires=[
    ],
    test_suite='tests',
    tests_require=[
    ],
    url='https://github.com/sluenenglish/dbio',
    version='0.0.0',
    zip_safe=False,
)

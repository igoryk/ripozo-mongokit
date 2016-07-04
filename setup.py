from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from setuptools import setup, find_packages

version = '0.0.1.dev1'

setup(
    name='ripozo-mongokit',
    version=version,
    packages=find_packages(include=['ripozo_mongokit', 'ripozo_mongokit.*']),
    url='https://github.com/igorkuksov/ripozo-mongokit',
    license='',
    author='IgorK',
    author_email='ikuksov@gmail.com',
    description='Integrates MongoKit with ripozo to easily create Mongo backed Hypermedia/HATEoAS/REST APIs',
    install_requires=[
        'ripozo',
        'mongokit',
        'pymongo<3.0'
        # 'bson' - https://api.mongodb.com/python/current/installation.html
    ],
    tests_require=[
        'unittest2',
        'tox',
        'mock',
        'pylint',
    ],
    test_suite="ripozo_mongokit_tests",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
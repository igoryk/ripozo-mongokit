ripozo-mongokit
===============

This package is a simple `ripozo <https://github.com/vertical-knowledge/ripozo>`_
extension that provides a Manger implementation to integrate ripozo_ with `mongokit <https://github.com/namlook/mongokit>`_.
It is capable of performing basic CRUD+L operation and fully implements
the `BaseManager <https://github.com/vertical-knowledge/ripozo/blob/master/ripozo/manager_base.py>`_ class that is provided in the ripozo_ package.

`ripozo documentation <http://ripozo.readthedocs.io/en/latest/>`_

`MongoKit documentation <https://github.com/namlook/mongokit/wiki>`_

Example
=======

Define a MongoKit document:

.. code-block:: python

    import datetime
    from mongokit import Document

    class Person(Document):
        structure = {
            'first_name': basestring,
            'second_name': basestring,
            'age': int,
            'date_created': datetime.datetime,
            'date_updated': datetime.datetime,
            }
         # You can define the document database and collection here


Then define a ripozo resource and extend MongoKitManager:

.. code-block:: python

    from ripozo-mongokit import MongoKitManager
    from mongokit import Connection

    connection = Connection()

    class PersonManager(MongoKitManager):
        model = Person

        # Alternatively, you can define the resource database and collection here
        # The properties below override __database__ and __collection__ attributes of
        # the model Document class
        database_name = 'user_data'
        collection_name = 'users'

    class PersonResource(ResourceBase):
        manager = PersonManager(connection)

Installation
============

:code:`pip install git+git://github.com/igorkuksov/ripozo-mongokit.git@master`


Known issues
============

The work is in progress on the following features:

1. Unit and Integration tests

2. Model field filtering

3. Possible python compatibility problems. Tested on 2.6 and 2.7.


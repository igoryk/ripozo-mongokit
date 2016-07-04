from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest2 as test
from bson.objectid import ObjectId
from mock.mock import Mock, MagicMock

from ripozo_mongokit import MongoKitManager
from mongokit import Document, Connection


class Manager(MongoKitManager):
    model = None
    id_field = 'id'
    exclude_fields = ('name',)


class MongoKitManagerTests(test.TestCase):
    """
    Tests for all MongoKitManager CRUDL methods
    """
    def __init__(self, *args, **kwargs):
        super(MongoKitManagerTests, self).__init__(*args, **kwargs)
        self.collection = MagicMock()
        self.connection = MagicMock(Model=self.collection, spec=Connection)
        self.model_cls = MagicMock(spec=Document, structure={'name': basestring})
        self.model_cls.__name__ = 'Model'
        Manager.model = self.model_cls

    def test_default_init(self):
        Manager(connection=self.connection)
        self.connection.register.assert_called_once_with([Manager.model])

    def test_db_collection(self):
        self.model_cls.__collection__ = 'c'
        self.model_cls.__database__ = 'd'
        Manager.database_name = 'a'
        Manager.collection_name = 'b'

        Manager(connection=self.connection)
        self.connection.register.assert_called_once_with([Manager.model])
        self.assertEqual([Manager.collection_name, Manager.database_name,
                          Manager.model.__collection__, Manager.model.__database__],
                         ['b', 'a', 'b', 'a'])
        with self.assertRaises(ValueError):
            Manager(connection=None)

    def test_create(self):
        manager = Manager(connection=self.connection)
        values = {'name': 'Joe'}
        document = MagicMock()
        document.__iter__.return_value = values
        self.collection.from_json.return_value = document
        manager.create(values)

        self.collection.from_json.assert_called_once_with('{"name": "Joe"}')
        document.save.assert_called_once()

    def test_retrieve(self):
        manager = Manager(connection=self.connection)
        self.assertEqual('a', manager._get_query('a'))
        self.assertEqual({}, manager._get_query(None))

        query = {
            'name': 'Jack',
        }
        lookup = {
            'second_name': ['Smith', 'Miller'],
            'address': {
                'line1': 'NYC',
                'line2': 'USA'
            },
            'age': 55,
            'id': '123456789012123456789012',
        }
        updated_query = {
            'name': 'Jack',
            'second_name': {'$in': ['Smith', 'Miller']},
            'address': {'line1': 'NYC', 'line2': 'USA'},
            'age': 55,
            '_id': ObjectId('123456789012123456789012')
        }
        self.collection.find_one.return_value = {
            'name': 'Jack',
            'second_name': 'Smith',
            'address': {
                'line1': ['1', '2'],
                'line2': 3
            },
            '_id': ObjectId('123456789012123456789012'),
        }

        obj = {
            'second_name': 'Smith',
            'address': {
                'line1': ['1', '2'],
                'line2': 3
            },
            'id': '123456789012123456789012',
        }

        self.assertEqual(manager.retrieve(lookup, query=query), obj)
        self.collection.find_one.assert_called_once_with(updated_query)

        objs = [{
            'name': 'John',
            '_id': ObjectId('123456789012123456789012')
        },
            {
                'name': 'Jim',
                '_id': ObjectId('123456789012123456789012')
            }]

        out_objs = [{
            'id': '123456789012123456789012',
        }, {
            'id': '123456789012123456789012'
        }]

        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter(objs))
        self.collection.find.return_value = cursor
        cursor.count.return_value = len(objs)

        self.assertEqual(manager.retrieve_all(lookup, query=query), (out_objs, dict(count=2)))
        self.collection.find.assert_called_once_with(updated_query)
        cursor.count.assert_called_once()

    def test_retreive_list(self):
        manager = Manager(connection=self.connection)

        filters = {
            manager.page_size_query_arg: 2,
            manager.page_query_arg: 2,
            manager.sort_query_arg: 'name,asc'
        }
        query = {
            'name': 'Joe',
            'age': 55,
            'city': ['NYC', 'PHY']
        }

        objs = [{
            'name': 'John',
            '_id': ObjectId('123456789012123456789011')
        }, {
            'name': 'Jim',
            '_id': ObjectId('123456789012123456789012')
        }, {
            'name': 'Jane',
            '_id': ObjectId('123456789012123456789013')
        }]

        out_objs = [{
            'id': '123456789012123456789011',
        }, {
            'id': '123456789012123456789012'
        }]

        page_object = {
            'page': {
                'totalPages': 6,
                'totalElements': 11,
                'number': 2,
                'size': 2
            }
        }

        links = {
            'next': {
                manager.page_query_arg: 3,
                manager.page_size_query_arg: 2
            },
            'prev': {
                manager.page_size_query_arg: 2,
                manager.page_query_arg: 1
            },
            'last': {
                manager.page_size_query_arg: 2,
                manager.page_query_arg: 5,
            },
            'first': {
                manager.page_size_query_arg: 2,
                manager.page_query_arg: 0
            }
        }

        anticipated_return = ({'page_object': page_object, 'data': out_objs, },
                              {'links': links, })

        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter(out_objs))
        find = Mock()
        skip = Mock()
        limit = Mock()
        self.collection.find.return_value = find
        find.sort.return_value = skip
        find.count.return_value = 11
        skip.count.return_value = 11
        skip.skip.return_value = limit
        find.skip.return_value = limit
        limit.limit.return_value = cursor

        self.assertEqual(manager.retrieve_list(filters, query=query), anticipated_return)

    def test_update(self):
        manager = Manager(connection=self.connection)
        doc = MagicMock()
        doc.__iter__.return_value = [('name', 'John'), ('age', 77)]
        data_list = MagicMock()
        data_list.__iter__ = Mock(return_value=iter([doc, doc]))
        self.collection.find.return_value = data_list

        updates = {'age': 77}
        lookup_keys = {'name': 'John'}

        updated = manager.update(lookup_keys, updates)

        doc.update.assert_called_with(updates)
        doc.save.assert_called()

        self.assertEqual(updated, [doc, doc])

    def test_delete(self):
        manager = Manager(connection=self.connection)
        doc = Mock()
        doc_list = MagicMock()
        doc_list.__iter__.return_value = [doc, doc]
        self.collection.find.return_value = doc_list

        self.assertEquals(manager.delete({}), {})

        doc.delete.assert_called()

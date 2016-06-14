"""
MongoKitManager
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import abc
import logging
import six

from types import NoneType
from functools import wraps
from bson import ObjectId
from mongokit import Connection
from ripozo.manager_base import BaseManager
from ripozo.resources.fields import IntegerField
from ripozo.utilities import make_json_safe


_logger = logging.getLogger(__name__)


def db_access(func):
    """
    Wraps a function that actually accesses the database.
    It injects a connection into the method and attempts to handle
    it after the function has run.

    :param method func: The method that is interacting with the database.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper responsible for handling connection
        """
        try:
            resp = func(self, self.connection, *args, **kwargs)
        except Exception as exc:
            raise exc
        else:
            return resp
    return wrapper


class MongoKitManager(six.with_metaclass(abc.ABCMeta, BaseManager)):
    """
    The MongoKitManager implements all the CRUDL methods.
    """
    all_fields = False
    fields = tuple()

    pagination_pk_query_arg = 'page'
    pagination_count_query_arg = 'size'

    paginate_by = 10

    # Database and collection can be overwritten in the model Document
    database_name = None
    collection_name = None

    def __init__(self, connection=Connection()):
        self.connection = connection

        # Overwrite database and collection of the model if they are defined
        if self.database_name and self.collection_name:
            self.model.__collection__ = self.collection_name
            self.model.__database__ = self.database_name
        connection.register([self.model])
        self.collection = getattr(connection, self.model.__name__)

    @abc.abstractproperty
    def model(self):
        raise NotImplementedError

    @classmethod
    def _get_field_type(cls, field_path):
        """

        Gets the field type from the cls.model class.

        :param field_path: string representation of the field in form of a dict path
        :return: field type as it defined in the model MongoKit document
        """

        field_type = NoneType
        try:
            fields = field_path.split('.')
            fields.reverse()
            current_dict = cls.model.structure
            while fields:
                field = fields.pop()
                current_dict = current_dict.__getitem__(field)
                field_type = current_dict if isinstance(current_dict, type) else dict
        except KeyError:
            _logger.error('Field %s is not defined in the %s document structure'
                          % (field_path, cls.model.__name__))

        return field_type

    @classmethod
    def _get_query(cls, lookup_keys):
        """

        Converts lookup dict into the query according to the model structure definition.

        :param lookup_keys: lookup keys
        :return: query usable in the find(), find_one(), and etc methods
        """
        # TODO convert keys to types from the cls.model.structure model mongokit definition
        query = {}
        if lookup_keys:
            for key, value in lookup_keys.iteritems():
                if key == '_id':
                    query[key] = ObjectId(value)
                else:
                    query[key] = value
        return query

    def _serialize_model(self, model, field_dict=None):
        """
        Takes a model and serializes the fields provided into
        a dictionary.

        :param Model model: The Sqlalchemy model instance to serialize
        :param dict field_dict: The dictionary of fields to return.
        :return: The serialized model.
        :rtype: dict
        """
        response = self._serialize_model_helper(model, field_dict=None)
        return make_json_safe(response)

    def _serialize_model_helper(self, model, field_dict=None):
        """
        A recursive function for serializing a model
        into a json ready format.
        """
        if isinstance(model, (list, set)):
            return [self._serialize_model(m) for m in model]

        if isinstance(model, ObjectId):
            return str(model)

        if isinstance(model, dict):
            model_dict = {}
            for name, sub in model.iteritems():
                value = model[name]
                if sub:
                    value = self._serialize_model(value, field_dict=None)
                model_dict[name] = value
            return model_dict

        return model

    @db_access
    def create(self, connection, values, *args, **kwargs):
        """
        Creates a new instance of a model object and saves it int the database.

        :param connection: injected by the wrapper
        :param values: dict with the values for the new entity
        :param args:
        :param kwargs:
        :return: dict
        """
        model_object = self.collection()
        model_object.update(values)
        model_object.save()
        return self._serialize_model(model_object)

    @db_access
    def retrieve(self, connection, lookup_keys, *args, **kwargs):
        """
        Retrieves a document according to the lookup_keys filters.

        :param connection: MongoKit connection, injected autmatically
        :param lookup_keys: query keys
        :return: dict, list
        """

        query = self._get_query(lookup_keys)
        res = self.collection.find_one(query)
        return self._serialize_model(res)

    @db_access
    def retrieve_list(self, connection, filters, *args, **kwargs):
        """
        Retrieves the list of documents according to the filters provieded.

        :param connection: MongoKit connection, injected automatically
        :param filters: dict, query filters
        :return: list, dict
        """

        translator = IntegerField('tmp')
        pagination_count = translator.translate(
            filters.pop(self.pagination_count_query_arg, self.paginate_by)
        )
        pagination_pk = translator.translate(
            filters.pop(self.pagination_pk_query_arg, 1)
        )
        pagination_pk -= 1  # logic works zero based. Pagination shouldn't be though

        query = self._get_query(filters)
        cursor = self.collection.find(query)
        count = cursor.count()

        query_skip = 0
        query_limit = self.paginate_by
        if pagination_pk:
            query_skip = pagination_pk * pagination_count
        if pagination_count:
            query_limit = pagination_count + 1

        next_link = None
        previous_link = None
        if count > pagination_count * (pagination_pk + 1):
            next_link = {self.pagination_pk_query_arg: pagination_pk + 2,
                         self.pagination_count_query_arg: pagination_count}

        if pagination_pk > 0:
            previous_link = {self.pagination_pk_query_arg: pagination_pk,
                             self.pagination_count_query_arg: pagination_count}

        model_objects = [obj for obj in cursor.skip(query_skip).limit(query_limit)]
        values = self._serialize_model(model_objects)
        return values, dict(links=dict(next=next_link, previous=previous_link))

    @db_access
    def update(self, connection, lookup_keys, updates, *args, **kwargs):

        """
        Retrieves the entities from the database according to the lookup_keys

        :param connection: injected by the wrapper
        :param updates: dict with the values for the new entity
        :return: dict
        :raises: SchemaTypeError
        """
        query = self._get_query(lookup_keys)
        model_objects = self.collection.find(query)
        count = model_objects.count()
        model_list = list()
        for model_object in model_objects:
            model_object.update(updates)
            model_object.save()
            model_list.append(model_object)

        model_dict = dict()
        if count > 1:
            model_dict['_embedded'] = model_list
        else:
            model_dict = model_list.pop()
        return self._serialize_model(model_dict)

    @db_access
    def delete(self, connection, lookup_keys, *args, **kwargs):
        """
        Deletes objects from the database.

        :param connection: connection is injected by the wrapper
        :param lookup_keys: query for the objects to delete
        :return: dict
        """

        query = self._get_query(lookup_keys)
        documents = self.collection.find(query)
        for doc in documents:
            doc.delete()

        return {}

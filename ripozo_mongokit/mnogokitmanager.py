"""
MongoKitManager
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import abc
import logging

import datetime
import six
import math

from types import NoneType
from bson import ObjectId
from bson.errors import InvalidId
from mongokit import Connection
from ripozo.manager_base import BaseManager
from ripozo.resources.fields import IntegerField

from ripozo_mongokit.fields import SortField

_logger = logging.getLogger(__name__)


class MongoKitManager(six.with_metaclass(abc.ABCMeta, BaseManager)):
    """
    The MongoKitManager implements all the CRUDL methods.
    """
    all_fields = True
    exclude_fields = tuple()

    id_field = None

    # Mapping between model and resource field names
    document_fields_map = None
    resource_fields_map = None

    page_query_arg = 'page'
    page_size_query_arg = 'size'
    sort_query_arg = 'sort'

    default_page_size = 10

    # Database and collection can be overwritten in the model Document
    database_name = None
    collection_name = None

    # Indicates that the ripozo_mongokit.RetrievePageList mixin is used.
    page_properties = False

    _connection = None

    def __init__(self, connection=None):
        self.connection = connection

        self.all_fields = (len(self.exclude_fields) == 0)

        # Overwrite database and collection of the model if they are defined
        if self.database_name and self.collection_name:
            self.model.__collection__ = self.collection_name
            self.model.__database__ = self.database_name
        self.connection.register([self.model])
        self.collection = getattr(self.connection, self.model.__name__)

        # Making a reverse map for fast resource -> model fields conversions
        if self.document_fields_map is not None:
            self.resource_fields_map = {}
            for model_f, resource_f in six.iteritems(self.document_fields_map):
                self.resource_fields_map[resource_f] = model_f

    @abc.abstractproperty
    def model(self):
        raise NotImplementedError

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        if isinstance(value, Connection):
            self._connection = value
        else:
            raise ValueError('Connection property must be of type mongokit.Connection')

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
        if isinstance(lookup_keys, dict):
            for key, value in six.iteritems(lookup_keys):
                if key == cls.id_field:
                    try:
                        query['_id'] = ObjectId(value)
                    except InvalidId:
                        query['_id'] = value
                else:
                    if isinstance(value, list):
                        query[key] = {'$in': [cls._get_query(v) for v in value]}
                    elif isinstance(value, dict):
                        query[key] = cls._get_query(value)
                    else:
                        query[key] = value
        else:
            query = lookup_keys if lookup_keys else {}
        return query

    def _serialize_model_helper(self, model):
        """
        A recursive function for serializing a model
        into a json ready format.
        """
        if model is None:
            return None

        if isinstance(model, ObjectId):
            return str(model)

        if isinstance(model, (list, set, tuple)):
            return [self._serialize_model_helper(m) for m in model]

        if isinstance(model, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
            return six.text_type(model)

        if isinstance(model, dict):
            for key, value in six.iteritems(model):
                model[key] = self._serialize_model_helper(value)

        return model

    def _replace_id(self, obj):
       if self.id_field and '_id' in obj:
           obj[self.id_field] = str(obj.pop('_id'))

    def _remove_excl_fields(self, obj):
        if not self.all_fields:
            for field in self.exclude_fields:
                if field in obj:
                    del obj[field]

    def _serialize_model(self, model):
        # Remove excluded fields
        if model is None:
            return {}

        # Replace default id field name
        if isinstance(model, (list, set, tuple)):
            for obj in model:
                self._replace_id(obj)
                self._remove_excl_fields(obj)
        else:
            self._replace_id(model)
            self._remove_excl_fields(model)

        return self._serialize_model_helper(model)

    def create(self, values, *args, **kwargs):
        """
        Creates a new instance of a model object and saves it int the database.

        :param values: dict with the values for the new entity
        :param args:
        :param kwargs:
        :return: dict
        """
        model_document = self.collection()
        model_document.update(values)
        model_document.save()
        return self._serialize_model(model_document)

    def retrieve(self, lookup_keys, *args, **kwargs):
        """
        Retrieves a document according to the lookup_keys filters.

        :param lookup_keys: query keys
        :return: dict, list
        """
        query = self._get_query(lookup_keys)
        if 'query' in kwargs:
            query.update(kwargs['query'])
        model_document = self.collection.find_one(query)
        return self._serialize_model(model_document)

    def retrieve_all(self, filters, *args, **kwargs):

        query = self._get_query(filters)
        if 'query' in kwargs:
            query.update(kwargs['query'])
        cursor = self.collection.find(query)
        count = cursor.count()

        values = [self._serialize_model(obj) for obj in cursor]

        return values, dict(count=count)

    def retrieve_list(self, filters, *args, **kwargs):
        """
        Retrieves the list of documents according to the filters provided.
        Pagination is zero based.

        :param filters: dict, query filters
        :return: list, dict
        """

        translator = IntegerField('tmp')
        page_size = translator.translate(
            filters.pop(self.page_size_query_arg, self.default_page_size)
        )
        page_number = translator.translate(
            filters.pop(self.page_query_arg, 0)
        )

        sort_tuple = SortField('sort').translate(filters.pop(self.sort_query_arg, None))

        query = self._get_query(filters)
        if 'query' in kwargs:
            query.update(kwargs['query'])
        cursor = self.collection.find(query).sort(sort_tuple[0], sort_tuple[1]) \
            if sort_tuple else self.collection.find(query)
        count = cursor.count()
        page_count = int(math.ceil(count / page_size))

        query_skip = 0
        query_limit = page_size
        if page_size and page_number:
            query_skip = page_size * page_number
            query_limit = page_size

        next_link = None
        previous_link = None
        first_link = None
        last_link = None

        if count > page_size * (page_number + 1):
            next_link = {self.page_query_arg: page_number + 1,
                         self.page_size_query_arg: page_size}

        if page_number > 0:
            previous_link = {self.page_query_arg: page_number - 1,
                             self.page_size_query_arg: page_size}
            first_link = {self.page_query_arg: 0,
                          self.page_size_query_arg: page_size}

        if page_number != (page_count - 1):
            last_link = {self.page_query_arg: page_count - 1,
                         self.page_size_query_arg: page_size}

        values = self._serialize_model([obj for obj in cursor.skip(query_skip).limit(query_limit)])
        page_object = dict(page=dict(size=page_size,
                                     totalElements=count,
                                     totalPages=page_count,
                                     number=page_number))
        return dict(data=values, page_object=page_object), \
               dict(links=dict(next=next_link,
                               prev=previous_link,
                               first=first_link,
                               last=last_link))

    def update(self, lookup_keys, updates, *args, **kwargs):

        """
        Retrieves the entities from the database according to the lookup_keys

        :param updates: dict with the values for the new entity
        :return: dict
        :raises: SchemaTypeError
        """
        query = self._get_query(lookup_keys)
        model_documents= self.collection.find(query)
        count = model_documents.count()
        model_list = list()
        for model_document in model_documents:
            model_document.update(updates)
            model_document.save()
            model_list.append(self._serialize_model(model_document))

        model_dict = dict()
        if count > 1:
            model_dict['_embedded'] = model_list
        else:
            model_dict = model_list.pop()
        return model_dict

    def delete(self, lookup_keys, *args, **kwargs):
        """
        Deletes objects from the database.

        :param lookup_keys: query for the objects to delete
        :return: dict
        """
        query = self._get_query(lookup_keys)
        documents = self.collection.find(query)
        for doc in documents:
            doc.delete()

        return {}

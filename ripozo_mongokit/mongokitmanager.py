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
import json

from bson import ObjectId
from bson.errors import InvalidId
from mongokit import Connection
from ripozo.manager_base import BaseManager
from ripozo.resources.fields import IntegerField

from ripozo_mongokit import export_name
from ripozo_mongokit.fields import SortField

_logger = logging.getLogger(__name__)


@export_name
class MongoKitManager(six.with_metaclass(abc.ABCMeta, BaseManager)):
    """
    The MongoKitManager implements all the CRUDL methods.

    :param iterable exclude_fields: a list of fields to exclude
        from the model.
    :param bool all_fields: Indicates whether some fields must be
        excluded from the model during serialization. For example,
        User.password_hash field is a good candidate for an exclusion.

    :param string id_field: allows to map between the standard Mongo
        '_id' ID field name and the name that is required by the client.
         For example, setting "id_field = 'user_id'" would make all
         the entities have 'user_id' field instead of standard '_id'

    :param string regex_suffix: the manager detects regex search terms
        by searching for the request parameters of the '*<regex_suffix>'
        format.

    :param string database_name:
    :param string collection_name: database and collection name
        params override corrspondent parameters of the Model document
        class.
    """
    all_fields = True
    exclude_fields = tuple()

    id_field = '_id'

    page_query_arg = 'page'
    page_size_query_arg = 'size'
    sort_query_arg = 'sort'

    regex_suffix = 'Regex'

    default_page_size = 10

    # Database and collection can be overwritten in the model Document
    database_name = None
    collection_name = None

    # Indicates that the ripozo_mongokit.RetrievePageList mixin is used.
    page_properties = False

    _connection = None

    def __init__(self, connection=None, *args, **kwargs):
        super(MongoKitManager, self).__init__(*args, **kwargs)
        self.connection = connection

        self.all_fields = (len(self.exclude_fields) == 0)

        # Overwrite database and collection of the model if they are defined
        if self.database_name and self.collection_name:
            self.model.__collection__ = self.collection_name
            self.model.__database__ = self.database_name
        self.connection.register([self.model])
        self.collection = getattr(self.connection, self.model.__name__)

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
    def _get_query(cls, lookup_keys):
        """
        Converts lookup dict into the query according to the model structure definition.

        :param lookup_keys: lookup keys
        :return: query usable in the find(), find_one(), and etc methods
        """
        query = {}
        if isinstance(lookup_keys, dict):
            for key, value in six.iteritems(lookup_keys):
                if key == cls.id_field:
                    try:
                        query['_id'] = ObjectId(value)
                    except InvalidId:
                        query['_id'] = value
                elif cls._is_regex_field(key):
                    query.update(cls._get_regex_query(key, value))
                else:
                    if isinstance(value, (list, tuple, set)):
                        query[key] = {'$in': [cls._get_query(v) for v in value]}
                    elif isinstance(value, dict):
                        query[key] = cls._get_query(value)
                    else:
                        query[key] = value
        else:
            query = lookup_keys if lookup_keys else {}
        return query

    @classmethod
    def _is_regex_field(cls, field):
        """
        Helper method that determines whether a given field corresponds
        to the regex field format and, therefore, is a regex field.

        :param field: field name
        :return: True if the field is a regex one
        """
        return field.endswith(cls.regex_suffix)

    @classmethod
    def _get_regex_query(cls, field, value):
        """
        Converts a regex field and its value to the regex query.

        :param field: request regex field name.
        :param value: regex query
        :return: dict - part of the Mongo query
        """

        return {field.replace(cls.regex_suffix, ''):{'$regex': str(value), '$options': 'i'}}

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
        :return: dict: serialized created document
        """
        model_document = self.collection.from_json(json.dumps(values))
        model_document.save()
        return self._serialize_model(model_document)

    def retrieve(self, lookup_keys, *args, **kwargs):
        """
        Retrieves a document according to the lookup_keys filters.

        :param lookup_keys: query keys
        :param kwargs: if kwargs dict contains a 'query' argument it is
            treated as ready MongoDB query dict.
        :return: dict: serialized json-ready document if found.
        """
        query = self._get_query(lookup_keys)
        if 'query' in kwargs:
            query.update(kwargs['query'])
        model_document = self.collection.find_one(query)
        return self._serialize_model(model_document)

    def retrieve_all(self, filters, *args, **kwargs):
        """
        Gets all entities according to filters without pagination.

        :param dict filters: pagination and query filters.
        :param kwargs: if kwargs dict contains a 'query' argument it is
            treated as ready MongoDB query dict.
        :return: tuple(list(dict)), dict): serialized structure, containing
            retrieved list of entities and a piece of metadata
        """
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
        Pagination is zero based. Supports ripozo_mongokit.RetrievePageList
        resource to mimic Spring-Data HATEoAS framework metadata.

        :param dict filters: pagination and query filters.
        :param kwargs: if kwargs dict contains a 'query' argument it is
            treated as ready MongoDB query dict.
        :return: tuple(list(dict)), dict): serialized structure, containing
            retrieved list of entities and a piece of metadata
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
        return dict(data=values, page_object=page_object), dict(links=dict(next=next_link,
                                                                           prev=previous_link,
                                                                           first=first_link,
                                                                           last=last_link))

    def update(self, filters, updates, *args, **kwargs):

        """
        Retrieves the entities from the database according to the filters.

        :param updates: dict with the values for the new entity
        :return: dict
        :raises: SchemaTypeError
        """
        query = self._get_query(updates)
        model_documents = self.collection.find(query)
        model_list = list()
        for model_document in model_documents:
            model_document.update(updates)
            model_document.save()
            model_list.append(self._serialize_model(model_document))

        return model_list

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

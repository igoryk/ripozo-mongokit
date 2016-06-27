from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ripozo import restmixins
from ripozo.decorators import apimethod, manager_translate

import logging

_logger = logging.getLogger(__name__)


class RetrievePageList(restmixins.RetrieveList):
    """
    Customized RetrieveList mixin to emulate Spring Data HATEOAS.
    Adds page properties to the resource and "last", "first", "prev", "next" links:

    {
        "page": {
            "size": 1,
            "totalElements": 7124,
            "totalPages": 7124,
            "number": 1
        },

        "_links": {
            "first": {
                "href": "/api/users?page=0&size=1
            },
            "last": {
                "href": "/api/users?page=7124&size=1
            },
            "next": {
                "href": "/api/users?page=2&size=1
            }
        },
        "_embedded": {
            "users": [
                {
                    "name": "John Doe",
                    "age": 45
                }
            ]
        }
    }

    """

    @apimethod(methods=['GET'], no_pks=True)
    @manager_translate(fields_attr='list_fields')
    def retrieve_list(cls, request):
        """
        A resource that contains the other resources as properties.

        :param RequestContainer request: The request in the standardized
            ripozo style.
        :return: An instance of the class
            that was called.
        :rtype: RetrieveList
        """
        _logger.debug('Retrieving list of resources using manager %s', cls.manager)
        props, meta = cls.manager.retrieve_list(request.query_args)
        if 'page_object' in props and 'data' in props:
            return_props = {cls.resource_name: props['data']}
            return_props.update(props['page_object'])
            return cls(properties=return_props, meta=meta,
                       status_code=200, query_args=cls.manager.fields, no_pks=True)
        else:
            return super(RetrievePageList, cls).retrieve_list(cls, request)


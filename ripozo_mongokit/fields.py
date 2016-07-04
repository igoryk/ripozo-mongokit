from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from ripozo.resources.fields.field import Field
from ripozo.resources.fields.validations import translate_iterable_to_single
from pymongo import ASCENDING, DESCENDING

import six


class SortField(Field):
    field_type = six.text_type

    def _translate(self, obj, skip_required=False):
        obj = translate_iterable_to_single(obj)
        if isinstance(obj, self.field_type):
            vals = obj.lower().split(',')
            if len(vals) == 2:
                return vals[0], ASCENDING if vals[1] == 'asc' else DESCENDING
        if obj is None:
            return obj
        raise ValueError('Not a valid sort option: %s' % obj)

    def _validate(self, obj, skip_required=False):
        return obj


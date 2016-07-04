"""
Integrates MongoKit with ripozo to
easily create mongo backed Hypermedia/HATEOAS/REST apis
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

__all__ = []


def export_name(fn):
    globals()[fn.__name__] = fn
    __all__.append(fn.__name__)
    return fn


from .mongokitmanager import *
from .restmixins import *

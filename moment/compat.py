#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['lru', 'msgpack', 'json', 'pickle', 'pickle_hi']


try:
    import msgpack
except ImportError:
    msgpack = None  # noqa

try:
    import lru
except ImportError:
    lru = None  # noqa

try:
    import ujson as json
except ImportError:
    try:
        import cjson as json  # noqa
    except ImportError:
        import json  # noqa


import pickle


class pickle_hi:

    @staticmethod
    def dumps(val):
        return pickle.dumps(val, pickle.HIGHEST_PROTOCOL)

    loads = staticmethod(pickle.loads)

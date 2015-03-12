#!/usr/bin/env python
# -*- coding: utf-8 -*-


__all__ = ['lru', 'json']

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

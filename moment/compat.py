#!/usr/bin/env python
# -*- coding: utf-8 -*-


__all__ = ['serializer', 'lru']


import msgpack as serializer

try:
    import lru
except ImportError:
    lru = None  # noqa

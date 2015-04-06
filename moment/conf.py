#!/usr/bin/env python
# -*- coding: utf-8 -*-


import redis
from redis.connection import PythonParser
from redis.connection import ConnectionPool

from .compat import json, pickle_hi, pickle, msgpack


__all__ = ['get_serializer', 'register_connection', 'get_connection']


MOMENT_KEY_PREFIX = 'spm'
MOMENT_SERIALIZER = 'json'


_serializers = {
    'json': json,
    'pickle': pickle,
    'pickle_hi': pickle_hi,
}
if msgpack:
    _serializers['msgpack'] = msgpack


def get_serializer(alias):
    alias = alias or MOMENT_SERIALIZER

    if hasattr(alias, 'loads'):
        return alias
    try:
        return _serializers[alias]
    except KeyError:
        raise LookupError("Serializer `{}` not configured.".format(alias))


_connections = {}


def register_connection(alias='default', host='localhost', port=6379, **kwargs):
    global _connections

    kwargs.setdefault('parser_class', PythonParser)
    kwargs.setdefault('db', 0)

    pool = ConnectionPool(host=host, port=port, **kwargs)
    conn = redis.StrictRedis(connection_pool=pool)

    _connections[alias] = conn
    return conn


def get_connection(alias='default'):
    global _connections

    if isinstance(alias, redis.StrictRedis):
        return alias

    try:
        return _connections[alias]
    except KeyError:
        raise LookupError("Connection `{}` not configured.".format(alias))

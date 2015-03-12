#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
from redis.connection import PythonParser
from redis.connection import ConnectionPool


__all__ = ['register_connection', 'get_connection', 'set_connection']


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

    return _connections[alias]


def set_connection(alias, connection):
    global _connections

    old_connection = _connections.pop(alias, None)
    _connections[alias] = connection
    return old_connection

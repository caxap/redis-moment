#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

__doc__ = """

Integration with django settings:

# settings.py

MOMENT_KEY_PREFIX = 'spm'
MOMENT_SERIALIZER = 'msgpack'
MOMENT_REDIS = {
    'default': {
        'host': 'localhost',
        'port': 6379,
        'db': 1,
        'socket_timeout': 3,
        'max_connections': 10,
    }
}

INSTALLED_APPS.append('moment.contrib.django')
"""

from .. import conf
from django.conf import settings


MOMENT_REDIS = getattr(settings, 'MOMENT_REDIS', None)
MOMENT_KEY_PREFIX = getattr(settings, 'MOMENT_KEY_PREFIX', None)
MOMENT_SERIALIZER = getattr(settings, 'MOMENT_SERIALIZER', None)


if MOMENT_KEY_PREFIX:
    conf.MOMENT_KEY_PREFIX = MOMENT_KEY_PREFIX

if MOMENT_SERIALIZER:
    conf.MOMENT_SERIALIZER = MOMENT_SERIALIZER

if MOMENT_REDIS:
    for alias, conn_conf in MOMENT_REDIS.items():
        conf.register_connection(alias, **conn_conf)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .base import Base
from .compat import json, lru
from .lua import (
    sequential_id as _sequential_id, multiset_union_update,
    multiset_intersection_update
)


__all__ = ['SimpleSequence', 'SimpleDict', 'SimpleCounter']


_NONE = object()


class BaseSequence(Base):
    """
    Tracks sequential ids for symbolic identifiers. Also optionaly holds
    cache of recenly created ids.

    Examples::

        seq = Sequence('sequence1')
        sid = seq.sequential_id('foo')
        'foo' in seq
    """
    cache_size = None
    clonable_attrs = ['cache_size']

    def __init__(self, name, client='default', cache_size=None):
        super(BaseSequence, self).__init__(name, client)
        self.cache_size = cache_size or self.cache_size

    @property
    def cache(self):
        if self.cache_size and lru:
            if not hasattr(self, '_cache'):
                self._cache = lru.LRU(self.cache_size)
            return self._cache

    def sequential_id(self, uuid, force=False):
        cache = self.cache
        if not force and cache:
            try:
                return cache[uuid]
            except KeyError:
                pass
        new_id = _sequential_id(self.key, uuid, self.client)
        if cache:
            cache[uuid] = new_id
        return new_id

    def has_uuid(self, uuid, force=False):
        cache = self.cache
        if not force and cache:
            try:
                # Is uuid was cached before?
                return cache[uuid] is not None
            except KeyError:
                pass
        # Seq id is zero-based.
        return self.client.zscore(self.key, uuid) is not None

    def count(self):
        return self.client.zcard(self.key)

    def delete(self):
        self.client.delete(self.key)
        self.flush_cache()

    def flush_cache(self):
        try:
            del self._cache
        except AttributeError:
            pass

    def __contains__(self, uuid):
        return self.has_uuid(uuid)

    def __len__(self):
        return self.count()


class MixinSerializable(object):

    serializer = None

    def dumps(self, value):
        return self.serializer.dumps(value)

    def loads(self, value):
        return self.serializer.loads(value)


class BaseDict(Base, MixinSerializable):
    clonable_attrs = ['serializer']

    def __init__(self, name, client='default', serializer=json):
        super(BaseDict, self).__init__(name, client)
        self.serializer = serializer

    def __len__(self):
        return self.client.hlen(self.key)

    def __contains__(self, key):
        return self.client.hexists(self.key, key)

    def __iter__(self):
        return self.iterkeys()

    def __setitem__(self, key, value):
        self.client.hset(self.key, key, self.dumps(value))

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key):
        if self.client.hdel(self.key, key) == 0:
            raise KeyError(key)

    def get(self, key, default=None):
        value = self.client.hget(self.key, key)
        if value is not None:
            return self.loads(value)
        return default

    def update(self, *args, **kwargs):
        for o in args:
            self._update(o)
        self._update(kwargs)

    def _update(self, other):
        if hasattr(other, 'items'):
            for k, v in other.items():
                self[k] = v
        else:
            for k, v in other:
                self[k] = v

    def keys(self):
        return self.client.hkeys(self.key)

    def values(self):
        return [self.loads(v) for v in self.client.hvals(self.key)]

    def items(self):
        data = self.client.hgetall(self.key)
        return [(k, self.loads(v)) for k, v in data.items()]

    def iterkeys(self):
        return iter(self.keys())

    def itervalues(self):
        return iter(self.values())

    def iteritems(self):
        return iter(self.items())

    def setdefault(self, key, value=None):
        if self.client.hsetnx(self.key, key, self.dumps(value)) == 1:
            return value
        return self.get(key)

    def has_key(self, key):
        return key in self

    def copy(self):
        return self.__class__(self.name, self.client, self.serializer)

    def clear(self):
        self.delete()

    def pop(self, key, default=_NONE):

        with self.client.pipeline() as pipe:
            pipe.hget(self.key, key)
            pipe.hdel(self.key, key)
            value, existed = pipe.execute()

        if not existed:
            if default is _NONE:
                raise KeyError(key)
            return default
        return self.loads(value)


class BaseCounter(BaseDict):

    def __init__(self, name, client='default', serializer=None):
        super(BaseCounter, self).__init__(name, client, None)

    def dumps(self, value):
        return str(int(value))

    def loads(self, value):
        return int(value)

    def _flatten(self, iterable, **kwargs):
        for k, v in self._merge(iterable, **kwargs):
            yield k
            yield v

    def _merge(self, iterable=None, **kwargs):
        if iterable:
            try:
                items = iterable.iteritems()
            except AttributeError:
                for k in iterable:
                    kwargs[k] = kwargs.get(k, 0) + 1
            else:
                for k, v in items:
                    kwargs[k] = kwargs.get(k, 0) + v
        return kwargs.items()

    def _update(self, iterable, multiplier, **kwargs):
        for k, v in self._merge(iterable, **kwargs):
            self.client.hincrby(self.key, k, v * multiplier)

    def update(self, iterable=None, **kwargs):
        self._update(iterable, 1, **kwargs)

    def subtract(self, iterable=None, **kwargs):
        self._update(iterable, -1, **kwargs)

    def intersection_update(self, iterable=None, **kwargs):
        args = self._flatten(iterable, **kwargs)
        multiset_intersection_update(keys=[self.key], args=args,
                                     client=self.client)

    def union_update(self, iterable=None, **kwargs):
        args = self._flatten(iterable, **kwargs)
        multiset_union_update(keys=[self.key], args=args,
                              client=self.client)

    def elements(self):
        for k, count in self.iteritems():
            for i in range(count):
                yield k

    def most_common(self, n=None):
        values = sorted(self.iteritems(), key=lambda v: v[1], reverse=True)
        if n:
            values = values[:n]
        return values

    def most_common_percent(self, n=None, precision=None):
        values = self.most_common()
        total = float(sum([v for _, v in values]))
        if n:
            values = values[:n]
        values = [(k, float(v) / total * 100) for k, v in values]
        if precision is not None:
            values = [(k, round(v, precision)) for k, v in values]
        return values

    def total(self):
        return sum(self.values())

    def __iadd__(self, other):
        self.update(other)
        return self

    def __isub__(self, other):
        self.subtract(other)
        return self

    def __iand__(self, other):
        self.intersection_update(other)
        return self

    def __ior__(self, other):
        self.union_update(other)
        return self


class SimpleSequence(BaseSequence):
    key_format = '{self.name}'


class SimpleDict(BaseDict):
    key_format = '{self.name}'


class SimpleCounter(BaseCounter):
    key_format = '{self.name}'

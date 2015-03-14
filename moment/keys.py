#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from . import conf
from .base import (
    _key, Base, MixinSerializable, BaseHour, BaseDay, BaseWeek, BaseMonth,
    BaseYear
)
from .timelines import _totimerange


__all__ = ['TIME_INDEX_KEY_NAMESAPCE', 'TimeIndexedKey', 'HourIndexedKey',
           'DayIndexedKey', 'WeekIndexedKey', 'MonthIndexedKey',
           'YearIndexedKey']


TIME_INDEX_KEY_NAMESAPCE = 'tik'


class TimeIndexedKey(MixinSerializable, Base):
    namespace = TIME_INDEX_KEY_NAMESAPCE
    clonable_attrs = ['serializer']
    key_format = '{self.name}'
    index_key_format = '{self.name}_index'

    def __init__(self, name, client='default', serializer=None):
        super(TimeIndexedKey, self).__init__(name, client)
        self.serializer = conf.get_serializer(serializer)

    @property
    def index_key(self):
        base_key = self.index_key_format.format(self=self)
        return _key(base_key, self.namespace)

    def value_key(self, key):
        return '{0}:{1}'.format(self.key, key)

    def __len__(self):
        return self.count()

    def __contains__(self, key):
        value_key = self.value_key(key)
        return self.client.exists(value_key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, key):
        value, timestamp = self.get(key)
        if value is None:
            raise KeyError(key)
        return value, timestamp

    def __delitem__(self, key):
        existed = self.remove(key)
        if not existed:
            raise KeyError(key)

    def set(self, key, value, timestamp=None, update_index=True):
        value_key, value = self.value_key(key), self.dumps(value)
        if not update_index:
            # TODO: check that index exists
            return self.client.set(value_key, value)
        else:
            timestamp = timestamp or time.time()
            with self.client.pipeline() as pipe:
                pipe.multi()
                pipe.zrem(self.index_key, key)
                pipe.zadd(self.index_key, timestamp, key)
                pipe.set(value_key, value)
                pipe.execute()

    def get(self, key):
        value_key = self.value_key(key)

        with self.client.pipeline() as pipe:
            pipe.zscore(self.index_key, key)
            pipe.get(value_key)
            timestamp, value = pipe.execute()

        if value is not None:
            return self.loads(value), timestamp
        return value, timestamp

    def remove(self, key):
        value_key = self.value_key(key)
        with self.client.pipeline() as pipe:
            pipe.multi()
            pipe.delete(value_key)
            pipe.zrem(self.index_key, key)
            existed, _ = pipe.execute()
        return existed

    def keys(self, start_time=None, end_time=None, limit=None,
             with_timestamp=False):
        start_time, end_time = _totimerange(start_time, end_time)
        offset = None if limit is None else 0
        items = self.client.zrangebyscore(self.index_key, start_time, end_time,
                                          offset, limit, with_timestamp)
        return items

    def timerange(self, start_time=None, end_time=None, limit=None):
        keys_with_timestamp = self.keys(start_time, end_time, limit, True)
        value_keys = [self.value_key(k) for k, _ in keys_with_timestamp]
        values = self.client.mget(*value_keys)
        result = []
        for (key, timestamp), value in zip(keys_with_timestamp, values):
            if value is not None:
                value = self.loads(value)
                result.append((key, value, timestamp))
        return result

    def count_timerange(self, start_time=None, end_time=None):
        start_time, end_time = _totimerange(start_time, end_time)
        return self.client.zcount(self.index_key, start_time, end_time)

    def delete_timerange(self, start_time=None, end_time=None):
        start_time, end_time = _totimerange(start_time, end_time)
        keys = self.keys(start_time, end_time)
        value_keys = [self.value_key(k) for k in keys]

        with self.client.pipeline() as pipe:
            pipe.delete(*value_keys)
            pipe.zremrangebyscore(self.index_key, start_time, end_time)
            pipe.execute()

    def has_key(self, key):
        return key in self

    def count(self):
        return self.client.zcard(self.index_key)

    def delete(self):
        value_key_pattern = self.value_key('*')
        keys = self.client.keys(value_key_pattern)
        self.client.delete(self.index_key, *keys)


class HourIndexedKey(BaseHour, TimeIndexedKey):
    pass


class DayIndexedKey(BaseDay, TimeIndexedKey):
    pass


class WeekIndexedKey(BaseWeek, TimeIndexedKey):
    pass


class MonthIndexedKey(BaseMonth, TimeIndexedKey):
    pass


class YearIndexedKey(BaseYear, TimeIndexedKey):
    pass

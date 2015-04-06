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
    """
    Key/value storage where keys indexed by time. This allows you continuously
    process newly created/updated keys.

    Examples ::

        index = TimeIndexedKey('users')

        # Save users profiles:
        index.set('uid1', user_data1, update_index=True)
        index.set('uid2', user_data2, update_index=True)

        # Get user ids added in the last 10 sec. (sorted by time added)
        result = index.keys(time.time() - 10, limit=2)
        for uid, timestamp in result:
            print 'User {0} added at {1}'.format(uid, timestamp)

        # Get assotiated data for these ids
        result = index.values('uid1', 'uid2')
        for uid, data in result:
             print 'User {0} -> {1}'.format(uid, data)

    """
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
        self.set(key, value, update_index=True)

    def __getitem__(self, key):
        value, timestamp = self.get(key)
        if value is None:
            raise KeyError(key)
        return value, timestamp

    def __delitem__(self, key):
        existed = self.remove(key)
        if not existed:
            raise KeyError(key)

    def set(self, key, value, timestamp=None, update_index=None):
        """ By default we trying to create index if it doesn't exist. """

        # If `update_index` is True force to update index
        if update_index:
            return self._set(key, value, timestamp)

        # If `update_index` is None create index if it not exists.
        index_time = self.client.zscore(self.index_key, key)
        if index_time is None and update_index is None:
            return self._set(key, value, timestamp)

        # Else just update assotiated value
        value_key, value = self.value_key(key), self.dumps(value)
        self.client.set(value_key, value)
        return index_time

    def _set(self, key, value, timestamp=None):
        timestamp = timestamp or time.time()
        value_key, value = self.value_key(key), self.dumps(value)

        with self.client.pipeline() as pipe:
            pipe.multi()
            pipe.zrem(self.index_key, key)
            pipe.zadd(self.index_key, timestamp, key)
            pipe.set(value_key, value)
            pipe.execute()
        return timestamp

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

    def values(self, *keys):
        assert keys, 'Al least one key should be given.'
        value_keys = [self.value_key(k) for k in keys]
        values = self.client.mget(*value_keys)
        result = []
        for key, value in zip(keys, values):
            if value is not None:
                value = self.loads(value)
                result.append((key, value))
        return result

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

        if value_keys:
            with self.client.pipeline() as pipe:
                pipe.delete(*value_keys)
                pipe.zremrangebyscore(self.index_key, start_time, end_time)
                pipe.execute()
        else:
            self.client.zremrangebyscore(self.index_key, start_time, end_time)

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

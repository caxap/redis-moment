#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from . import conf
from .base import Base, BaseHour, BaseDay, BaseWeek, BaseMonth, BaseYear
from .collections import MixinSerializable

__all__ = ['TIMELINE_NAMESPACE', 'TIMELINE_ALIASES', 'Timeline',
           'HourTimeline', 'DayTimeline', 'WeekTimeline',
           'MonthTimeline', 'YearTimeline']


TIMELINE_NAMESPACE = 'tln'


def _totimerange(start_time, end_time):
    if start_time is None:
        start_time = '-inf'
    if end_time is None:
        end_time = '+inf'
    return start_time, end_time


class Timeline(Base, MixinSerializable):
    namespace = TIMELINE_NAMESPACE
    key_format = '{self.name}'
    clonable_attrs = ['serializer']

    def __init__(self, name, client='default', serializer=None):
        super(Timeline, self).__init__(name, client)
        self.serializer = conf.get_serializer(serializer)

    def encode(self, data, timestamp):
        return {'d': data, 't': timestamp}

    def decode(self, value):
        return value.get('d'), value.get('t')

    def add(self, *items, **kwargs):
        """
        Add new item to `timeline`

        Examples ::

            tl = Timeline('events')
            tl.add('event1', 'event2', timestamp=time.time())
        """
        assert items, 'At least one item should be given.'

        timestamp = kwargs.get('timestamp') or time.time()
        args = []
        for item in items:
            args.append(timestamp)
            args.append(self.dumps(self.encode(item, timestamp)))
        self.client.zadd(self.key, *args)
        return timestamp

    def timerange(self, start_time=None, end_time=None, limit=None):
        start_time, end_time = _totimerange(start_time, end_time)
        offset = None if limit is None else 0
        items = self.client.zrangebyscore(self.key, start_time,
                                          end_time, offset, limit)
        return [self.decode(self.loads(i)) for i in items]

    def delete_timerange(self, start_time=None, end_time=None):
        start_time, end_time = _totimerange(start_time, end_time)
        return self.client.zremrangebyscore(self.key, start_time, end_time)

    def count_timerange(self, start_time=None, end_time=None):
        start_time, end_time = _totimerange(start_time, end_time)
        return self.client.zcount(self.key, start_time, end_time)

    def range(self, start=0, end=-1):
        items = self.client.zrange(self.key, start, end)
        return [self.decode(self.loads(i)) for i in items]

    def delete_range(self, start=0, end=-1):
        return self.client.zremrangebyrank(self.key, start, end)

    def head(self, limit=1):
        return self.range(0, limit - 1)

    def tail(self, limit=1):
        return self.range(-limit, -1)

    def items(self):
        return self.range()

    def count(self):
        return self.client.zcard(self.key)

    def __len__(self):
        return self.count()


class HourTimeline(BaseHour, Timeline):
    pass


class DayTimeline(BaseDay, Timeline):
    pass


class WeekTimeline(BaseWeek, Timeline):
    pass


class MonthTimeline(BaseMonth, Timeline):
    pass


class YearTimeline(BaseYear, Timeline):
    pass


TIMELINE_ALIASES = {
    'hour': HourTimeline,
    'day': DayTimeline,
    'week': WeekTimeline,
    'month': MonthTimeline,
    'year': YearTimeline,
}

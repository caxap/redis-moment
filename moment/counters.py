#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
from datetime import datetime
from .connections import get_connection
from .collections import BaseCounter
from .base import BaseHour, BaseDay, BaseWeek, BaseMonth, BaseYear

__all__ = ['COUNTER_NAMESPACE', 'COUNTER_ALIASES', 'update_all', 'Counter',
           'HourCounter', 'DayCounter', 'WeekCounter', 'MonthCounter',
           'YearCounter']


COUNTER_NAMESPACE = 'cnt'


def update_all(counter_names, iterable=None, counter_types=None, dt=None,
               client='default'):
    """
    Updates counters for hours, days, weeks and months. Default counter
    type is `Day`.

    Examples::

        update_all('counter1', {'value1': 1, 'value2': 1})
        update_all(['counter1', 'counter2'], {'value1': 2, 'value2': 3})
        update_all(['counter1', 'counter2'], ['value1', 'value2'])
        update_all('counter1', 'value1')
    """
    client = get_connection(client)

    if isinstance(counter_names, basestring):
        counter_names = [counter_names]

    if isinstance(iterable, basestring):
        iterable = [iterable]

    # `counter_types` may be string, event class or events list
    if counter_types is None:
        counter_types = [DayCounter]
    elif (isinstance(counter_types, basestring) or
            not isinstance(counter_types, (list, tuple, set))):
        counter_types = [counter_types]
    # Resolve counters aliasses
    counter_types = [COUNTER_ALIASES.get(t, t) for t in counter_types]

    if dt is None:
        dt = datetime.utcnow()

    # TODO: use pipe and multi commands
    counters = []
    for name, cn_type in itertools.product(counter_names, counter_types):
        counters.append(cn_type.from_date(name, dt, client))

    for counter in counters:
        counter.update(iterable)

    return counters


class Counter(BaseCounter):
    namespace = COUNTER_NAMESPACE
    key_format = '{self.name}'


class HourCounter(BaseHour, Counter):
    pass


class DayCounter(BaseDay, Counter):
    pass


class WeekCounter(BaseWeek, Counter):
    pass


class MonthCounter(BaseMonth, Counter):
    pass


class YearCounter(BaseYear, Counter):
    pass


COUNTER_ALIASES = {
    'hour': HourCounter,
    'day': DayCounter,
    'week': WeekCounter,
    'month': MonthCounter,
    'year': YearCounter,
}

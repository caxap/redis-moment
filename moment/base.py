#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['KEY_PREFIX', '_key']

import calendar
import inspect
from datetime import datetime, date, timedelta
from .connections import get_connection
from .utils import not_none, add_month, iso_to_gregorian


KEY_PREFIX = 'spm'


def _key(name, namespace=None, prefix=KEY_PREFIX, delim=':'):
    return (delim or ':').join(filter(None, [prefix, namespace, name]))


def _require_defined(parent_cls, instance, name, kind='property',
                     raise_cls=NotImplementedError):
    if not hasattr(instance, name):
        parent_name = parent_cls.__name__   # noqa
        child_name = instance.__class__.__name__  # noqa
        msg = ("`{child_name}` subclass of `{parent_name}` should define "
               "`{name}` {kind}.".format(**locals()))
        raise raise_cls(msg)


class MixinPeriod(object):

    def next(self):
        return self.delta(value=1)

    def prev(self):
        return self.delta(value=-1)

    def delta(self, value):
        _require_defined(MixinPeriod, self, 'delta', 'method')


class MixinClonable(object):

    clonable_attrs = []

    def get_clonable_attrs(self):
        all_clonable_attrs = []
        for base in inspect.getmro(self.__class__):
            attrs = getattr(base, 'clonable_attrs', None)
            if attrs:
                all_clonable_attrs.extend(attrs)
        return list(set(all_clonable_attrs))

    def clone(self, **initials):
        names = self.get_clonable_attrs()
        attrs = {n: getattr(self, n) for n in names} if names else {}
        attrs = dict(attrs, **initials)
        instance = self.__class__(self.name, client=self.client)
        for name, value in attrs.items():
            setattr(instance, name, value)
        return instance


class Base(MixinClonable):

    def __init__(self, name, client='default'):
        self.name = name
        self.client = client

    def client():
        def fget(self):
            return self._client

        def fset(self, client):
            """ Automatically resolve connection by alias. """
            self._client = get_connection(client)
        return locals()

    client = property(**client())

    @property
    def key(self):
        _require_defined(Base, self, 'key_format')
        base_key = self.key_format.format(self=self)
        return _key(base_key, getattr(self, 'namespace', None))

    def delete(self):
        self.client.delete(self.key)

    def expire(self, ttl):
        self.client.expire(self.key, ttl)

    def __bool__(self):
        return self.client.exists(self.key)

    __nonzero__ = __bool__

    def __eq__(self, other):
        other_key = getattr(other, 'key', None)
        return other_key is not None and self.key == other_key

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.key)

    __repr__ = __str__


class MixinPeriod(object):

    def next(self):
        return self.delta(value=1)

    def prev(self):
        return self.delta(value=-1)

    def delta(self, value):
        _require_defined(MixinPeriod, self, 'delta', 'method')


class BaseHour(Base, MixinPeriod):

    # Example: 'active:2015-03-13-09'
    key_format = '{self.name}:{self.year:02d}-{self.month:02d}-{self.day:02d}-{self.hour:02d}'
    clonable_attrs = ['year', 'month', 'day', 'hour']

    @classmethod
    def from_date(cls, name, dt=None, client='default', **kwargs):
        if dt is None:
            dt = datetime.utcnow()
        return cls(name, dt.year, dt.month, dt.day, dt.hour, client, **kwargs)

    def __init__(self, name, year=None, month=None, day=None, hour=None,
                 client='default', **kwargs):
        super(BaseHour, self).__init__(name, client, **kwargs)
        self.set_period(year, month, day, hour)

    def set_period(self, year=None, month=None, day=None, hour=None):
        now = datetime.utcnow()
        self.year = not_none(year, now.year)
        self.month = not_none(month, now.month)
        self.day = not_none(day, now.day)
        self.hour = not_none(hour, now.hour)

    def delta(self, value):
        dt = datetime(self.year, self.month, self.day, self.hour) + timedelta(hours=value)
        return self.clone(year=dt.year, month=dt.month, day=dt.day, hour=dt.hour)

    def period_start(self):
        return datetime(self.year, self.month, self.day, self.hour)

    def period_end(self):
        return datetime(self.year, self.month, self.day, self.hour, 59, 59, 999999)


class BaseDay(Base, MixinPeriod):

    # Example: 'active:2015-03-13'
    key_format = '{self.name}:{self.year}-{self.month:02d}-{self.day:02d}'
    clonable_attrs = ['year', 'month', 'day']

    @classmethod
    def from_date(cls, name, dt=None, client='default', **kwargs):
        if dt is None:
            dt = datetime.utcnow()
        return cls(name, year=dt.year, month=dt.month, day=dt.day, client=client, **kwargs)

    def __init__(self, name, year=None, month=None, day=None, client='default', **kwargs):
        super(BaseDay, self).__init__(name, client, **kwargs)
        self.set_period(year, month, day)

    def set_period(self, year=None, month=None, day=None):
        print year, month, day
        now = datetime.utcnow()
        self.year = not_none(year, now.year)
        self.month = not_none(month, now.month)
        self.day = not_none(day, now.day)

    def delta(self, value):
        dt = date(self.year, self.month, self.day) + timedelta(days=value)
        return self.clone(year=dt.year, month=dt.month, day=dt.day)

    def period_start(self):
        return datetime(self.year, self.month, self.day)

    def period_end(self):
        return datetime(self.year, self.month, self.day, 23, 59, 59, 999999)


class BaseMonth(Base, MixinPeriod):

    # Example: 'active:2015-03'
    key_format = '{self.name}:{self.year}-{self.month:02d}'
    clonable_attrs = ['year', 'month']

    @classmethod
    def from_date(cls, name, dt=None, client='default', **kwargs):
        if dt is None:
            dt = datetime.utcnow()
        return cls(name, dt.year, dt.month, client, **kwargs)

    def __init__(self, name, year=None, month=None, client='default', **kwargs):
        super(BaseMonth, self).__init__(name, client, **kwargs)
        self.set_period(year, month)

    def set_period(self, year=None, month=None):
        now = datetime.utcnow()
        self.year = not_none(year, now.year)
        self.month = not_none(month, now.month)

    def delta(self, value):
        year, month = add_month(self.year, self.month, value)
        return self.clone(year=year, month=month)

    def period_start(self):
        return datetime(self.year, self.month, 1)

    def period_end(self):
        _, day = calendar.monthrange(self.year, self.month)
        return datetime(self.year, self.month, day, 23, 59, 59, 999999)


class BaseWeek(Base, MixinPeriod):

    # Example: 'active:2015-W35'
    key_format = '{self.name}:{self.year}-W{self.week:02d}'
    clonable_attrs = ['year', 'week']

    @classmethod
    def from_date(cls, name, dt=None, client='default', **kwargs):
        if dt is None:
            dt = datetime.utcnow()
        dt_year, dt_week, _ = dt.isocalendar()
        return cls(name, dt_year, dt_week, client, **kwargs)

    def __init__(self, name, year=None, week=None, client='default', **kwargs):
        super(BaseWeek, self).__init__(name, client, **kwargs)
        self.set_period(year, week)

    def set_period(self, year=None, week=None):
        now = datetime.utcnow()
        now_year, now_week, _ = now.isocalendar()
        self.year = not_none(year, now_year)
        self.week = not_none(week, now_week)

    def delta(self, value):
        dt = iso_to_gregorian(self.year, self.week + value, 1)
        year, week, _ = dt.isocalendar()
        return self.__class__(self.name, year, week, self.client)

    def period_start(self):
        s = iso_to_gregorian(self.year, self.week, 1)  # mon
        return datetime(s.year, s.month, s.day)

    def period_end(self):
        e = iso_to_gregorian(self.year, self.week, 7)  # mon
        return datetime(e.year, e.month, e.day, 23, 59, 59, 999999)


class BaseYear(Base, MixinPeriod):

    # Example: 'active:2015-W35'
    key_format = '{self.name}:{self.year}'
    clonable_attrs = ['year']

    @classmethod
    def from_date(cls, name, dt=None, client='default', **kwargs):
        if dt is None:
            dt = datetime.utcnow()
        return cls(name, dt.year, client, **kwargs)

    def __init__(self, name, year=None, client='default', **kwargs):
        super(BaseYear, self).__init__(name, client, **kwargs)
        self.set_period(year)

    def set_period(self, year=None):
        now = datetime.utcnow()
        self.year = not_none(year, now.year)

    def delta(self, value):
        return self.clone(year=self.year + value)

    def period_start(self):
        return datetime(self.year, 1, 1)

    def period_end(self):
        return datetime(self.year, 12, 31, 23, 59, 59, 999999)

# -*- coding: utf-8 -*-

import itertools
from datetime import datetime
from . import conf
from .base import _key, Base, BaseHour, BaseDay, BaseWeek, BaseMonth, BaseYear
from .collections import BaseSequence
from .lua import msetbit


__all__ = ['EVENT_NAMESPACE', 'EVENT_ALIASES', 'SEQUENCE_NAMESPACE',
           'record_events', 'delete_temporary_bitop_keys', 'Sequence',
           'Event', 'HourEvent', 'DayEvent', 'MonthEvent', 'WeekEvent',
           'YearEvent', 'Or', 'And', 'Xor', 'Not', 'LDiff']


EVENT_NAMESPACE = 'evt'
SEQUENCE_NAMESPACE = 'seq'


def record_events(uuids, event_names, event_types=None, dt=None, client='default',
                  sequence=None):
    """
    Records events for hours, days, weeks and months.

    Examples::

        seq = Sequence('sequence1')
        record_events('foo_id', 'event1')
        record_events('foo_id', 'event1', 'day', sequence=seq)
        record_events('foo_id', 'event1', MonthEvent, 'sequence1')
        record_events('foo_id', ['event1', 'event2'], [DayEvent, MonthEvent], seq)
        record_events('foo_id', ['event1', 'event2'], ['day', 'month'], 'sequence1')
    """
    client = conf.get_connection(client)

    if not isinstance(uuids, (list, tuple, set)):
        uuids = [uuids]

    if not isinstance(event_names, (list, tuple, set)):
        event_names = [event_names]

    # `event_types` may be string, event class or events list
    if event_types is None:
        event_types = [DayEvent]
    elif (isinstance(event_types, basestring) or
            not isinstance(event_types, (list, tuple, set))):
        event_types = [event_types]
    # Resolve event aliasses
    event_types = [EVENT_ALIASES.get(t, t) for t in event_types]

    if dt is None:
        dt = datetime.utcnow()

    events = []
    for name, ev_type in itertools.product(event_names, event_types):
        events.append(ev_type.from_date(name, dt, client, sequence=sequence))

    first = events[0]
    keys = [ev.key for ev in events]
    # TODO: make single `msetbit` call
    for uuid in uuids:
        if len(events) == 1:
            # For single event just set bit directly
            first.record(uuid)
        else:
            # Because sequence the same for all events
            sid = first.sequential_id(uuid)
            msetbit(keys=keys, args=([sid, 1] * len(keys)), client=client)

    return events


class Sequence(BaseSequence):
    cache_size = 100
    namespace = SEQUENCE_NAMESPACE
    key_format = '{self.name}'


class MixinBitwise(object):

    def __invert__(self):
        return Not(self.client, self)

    def __or__(self, other):
        return Or(self.client, self, other)

    def __and__(self, other):
        return And(self.client, self, other)

    def __xor__(self, other):
        return Xor(self.client, self, other)

    def __sub__(self, other):
        return LDiff(self.client, self, other)


class Event(Base, MixinBitwise):
    namespace = EVENT_NAMESPACE
    key_format = '{self.name}'
    clonable_attrs = ['sequence']

    def __init__(self, name, client='default', sequence=None):
        super(Event, self).__init__(name, client)
        self.sequence = sequence

    def sequence():
        def fget(self):
            return self._sequence

        def fset(self, sequence):
            """ Automatically create `Sequence` instance by name. """
            if isinstance(sequence, basestring):
                sequence = Sequence(sequence, self.client)
            self._sequence = sequence

        return locals()

    sequence = property(**sequence())

    def sequential_id(self, uuid):
        if self.sequence is not None:
            return self.sequence.sequential_id(uuid)
        try:
            return int(uuid)
        except (ValueError, TypeError):
            raise ValueError("A `Sequence` instance is required "
                             "to use non integer uuid `%s`." % (uuid,))

    def is_recorded(self, uuid):
        if self.sequence is not None and uuid not in self.sequence:
            return False
        sid = self.sequential_id(uuid)
        return bool(self.client.getbit(self.key, sid))

    def record(self, uuid):
        return self.client.setbit(self.key, self.sequential_id(uuid), 1)

    def count(self):
        return self.client.bitcount(self.key)

    def delete(self, cascade=False):
        self.client.delete(self.key)
        if cascade and self.sequence is not None:
            self.sequence.delete()

    def __len__(self):
        return self.count()

    def __contains__(self, uuid):
        return self.is_recorded(uuid)

    def __eq__(self, other):
        other_key = getattr(other, 'key', None)
        other_sequence = getattr(other, 'sequence', None)
        return (other_key is not None and
                self.key == other_key and
                self.sequence == other_sequence)


class HourEvent(BaseHour, Event):
    pass


class DayEvent(BaseDay, Event):
    pass


class WeekEvent(BaseWeek, Event):
    pass


class MonthEvent(BaseMonth, Event):
    pass


class YearEvent(BaseYear, Event):

    def __init__(self, name, year=None, client='default', sequence=None):
        super(YearEvent, self).__init__(name, client, year)
        self.sequence = sequence

    def months(self):
        if not hasattr(self, '_months'):
            month = lambda i: MonthEvent(self.name, self.year, i, self.client, self.sequence)
            self._months = Or(*[month(i) for i in range(1, 13)])
        return self._months

    @property
    def key(self):
        return self.months.key

    def delete(self, cascade=False):
        self.client.delete(self.key)
        if cascade:
            self.months.delete(cascade=cascade)


class BitOperation(Event):
    """
    Base class for bit operations (AND, OR, XOR, NOT).

    Please note that each bit operation creates a new key  prefixed with
    `spm:bitop_`. Bit operations can be nested.

    Examples::

        s1 = Sequence('events')
        m2 = Month('event1', 2015, 2, s1)
        m3 = Month('event1', 2015, 3, s1)
        m2 & m3 == And(m2, m3)
    """
    def __init__(self, op_name, client_or_event, *events):
        if hasattr(client_or_event, 'key'):
            events = list(events)
            events.insert(0, client_or_event)
            client = 'default'
        else:
            client = client_or_event

        cls_name = self.__class__.__name__
        assert events, \
            "At least one event should be given to perform `%s` operation." % (cls_name,)

        sequences = [ev.sequence for ev in events]
        s1 = sequences[0]
        for s in sequences[1:]:
            if s != s1:
                raise ValueError("Event sequences mismatch (%s != %s)" % (s1, s))

        name = 'bitop_{0}'.format(op_name)
        super(BitOperation, self).__init__(name, client, sequences[0])

        self.op_name = op_name
        self.events = events
        self.event_keys = [ev.key for ev in events]
        self.evaluate()

    @property
    def key(self):
        k = '{0.name}:({1})'
        return _key(k.format(self, '~'.join(self.event_keys)))

    def evaluate(self):
        self.client.bitop(self.op_name, self.key, *self.event_keys)

    def delete(self, cascade=False):
        self.client.delete(self.key)
        if cascade:
            for ev in self.events:
                if isinstance(ev, BitOperation):
                    ev.delete(cascade=cascade)

    def clone(self, **initials):
        raise NotImplementedError(
            'Method `clone()` is not implemented for `BitOperation` class.')


class And(BitOperation):

    def __init__(self, client_or_event, *events):
        super(And, self).__init__('AND', client_or_event, *events)


class Or(BitOperation):

    def __init__(self, client_or_event, *events):
        super(Or, self).__init__('OR', client_or_event, *events)


class Xor(BitOperation):

    def __init__(self, client_or_event, *events):
        super(Xor, self).__init__('XOR', client_or_event, *events)


class Not(BitOperation):

    def __init__(self, client_or_event, *events):
        super(Not, self).__init__('NOT', client_or_event, *events)


# TODO: rewrite to lua script
class LDiff(BitOperation):
    """
    Left diff bitwise operation:

    LDiff(A, B, C) == A - (B & C) == A & ~(B & C)
    """
    def __init__(self, client_or_event, *events):
        assert len(events) > 1, \
            "At least two events should be given to perform `LDiff` operation."
        super(LDiff, self).__init__('_LDIFF', client_or_event, *events)

    def evaluate(self):
        left, tail = self.events[0], self.events[1:]
        if len(tail) > 1:
            right = Not(And(self.client, *tail))
        else:
            right = Not(self.client, tail[0])
        self.client.bitop('AND', self.key, left.key, right.key)


EVENT_ALIASES = {
    'hour': HourEvent,
    'day': DayEvent,
    'week': WeekEvent,
    'month': MonthEvent,
    #'year': YearEvent,
}


def delete_temporary_bitop_keys(client='default', dryrun=False):
    """ Delete all temporary keys that are used when using bit operations. """
    client = conf.get_connection(client)
    pattertn = '{}:bitop_*'.format(EVENT_NAMESPACE)
    keys = client.keys(_key(pattertn))
    if not dryrun and len(keys) > 0:
        client.delete(*keys)
    return keys

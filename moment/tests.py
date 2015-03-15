#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import uuid
import unittest

from . import conf
from . import timelines
from . import keys


client = conf.register_connection()


##############################################################################
# Timeline Tests
##############################################################################

class TimelineTestCase(unittest.TestCase):

    timeline_class = timelines.Timeline

    def setup_timeline(self):
        self.timeline = self.timeline_class('test')
        self.start_time = int(time.time())
        self.items = []

        for i, t in enumerate(range(self.start_time, self.start_time + 10)):
            item = {'index': i}
            self.items.append((item, t))
            self.timeline.add(item, timestamp=t)

    def teardown_timeline(self):
        self.timeline.delete()

    def assert_ranges_equal(self, items1, items2):
        self.assertEqual(len(items1), len(items2))
        for (item1, t1), (item2, t2) in zip(items1, items2):
            self.assertEqual(t1, t2)
            self.assertEqual(item1, item2)

    def setUp(self):
        self.setup_timeline()

    def tearDown(self):
        self.teardown_timeline()

    def test_count(self):
        self.assertEqual(self.timeline.count(), len(self.items))
        self.assertEqual(len(self.timeline), len(self.items))

    def test_tail(self):
        last1, t1 = self.items[-1]
        last2, t2 = self.timeline.tail()[0]
        self.assertEqual(t1, t2)
        self.assertEqual(last1, last2)

        tail1 = self.items[-2:]
        tail2 = self.timeline.tail(2)
        self.assert_ranges_equal(tail1, tail2)

    def test_head(self):
        first1, t1 = self.items[0]
        first2, t2 = self.timeline.head()[0]
        self.assertEqual(t1, t2)
        self.assertEqual(first1, first2)

        head1 = self.items[:2]
        head2 = self.timeline.head(2)
        self.assert_ranges_equal(head1, head2)

    def test_range(self):
        self.assert_ranges_equal(self.items[2:4], self.timeline.range(2, 3))
        self.assert_ranges_equal(self.items[2:], self.timeline.range(2))
        self.assert_ranges_equal(self.items[2:-2], self.timeline.range(2, -3))
        self.assert_ranges_equal(self.items[-4:-1], self.timeline.range(-4, -2))

    def test_delete_range(self):
        items1 = self.items[:2] + self.items[4:]
        self.timeline.delete_range(2, 3)
        items2 = self.timeline.items()
        self.assertEqual(len(items2), len(self.items) - 2)
        self.assert_ranges_equal(items1, self.timeline.items())

    def test_timerange(self):
        items1 = self.items[2:5]
        start, end = self.items[2], self.items[4]
        start_ts, end_ts = start[1], end[1]
        items2 = self.timeline.timerange(start_ts, end_ts)
        self.assert_ranges_equal(items1, items2)

    def test_count_timerange(self):
        items1 = self.items[2:5]
        start, end = self.items[2], self.items[4]
        start_ts, end_ts = start[1], end[1]
        self.assertEqual(len(items1), self.timeline.count_timerange(start_ts, end_ts))

    def test_delete_timerange(self):
        items1 = self.items[:2] + self.items[4:]
        start, end = self.items[2], self.items[3]
        start_ts, end_ts = start[1], end[1]
        self.timeline.delete_timerange(start_ts, end_ts)
        items2 = self.timeline.items()
        self.assertEqual(len(items2), len(self.items) - 2)
        self.assert_ranges_equal(items1, self.timeline.items())


class HourTimelineTestCase(TimelineTestCase):
    timeline_class = timelines.HourTimeline


class DayTimelineTestCase(TimelineTestCase):
    timeline_class = timelines.DayTimeline


class WeekTimelineTestCase(TimelineTestCase):
    timeline_class = timelines.WeekTimeline


class MonthTimelineTestCase(TimelineTestCase):
    timeline_class = timelines.MonthTimeline


class YearTimelineTestCase(TimelineTestCase):
    timeline_class = timelines.YearTimeline


##############################################################################
# Time Indexed Keys Tests
##############################################################################

class TimeIndexedKeyTestCase(unittest.TestCase):

    key_class = keys.TimeIndexedKey

    def setup_indexed_key(self):
        self.tik = self.key_class('test')
        self.start_time = int(time.time())
        self.items = []

        for i, ts in enumerate(range(self.start_time, self.start_time + 10)):
            key = str(uuid.uuid4().hex)[:5]
            value = {'index': i}
            self.items.append((key, value, ts))
            self.tik.set(key, value, timestamp=ts, update_index=True)

    def teardown_indexed_key(self):
        self.tik.delete()

    def assert_ranges_equal(self, items1, items2):
        self.assertEqual(len(items1), len(items2))
        for (k1, v1, t1), (k2, v2, t2) in zip(items1, items2):
            self.assertEqual((k1, v1, t1), (k2, v2, t2))

    def setUp(self):
        self.setup_indexed_key()

    def tearDown(self):
        self.teardown_indexed_key()

    def test_get_set(self):
        now = int(time.time())
        key = str(uuid.uuid4().hex)[:5]
        val = {'foo': 1}
        self.tik.set(key, val, timestamp=now, update_index=True)
        val1, t1 = self.tik.get(key)
        val2, t2 = self.tik[key]
        self.assertEqual(now, t1, t2)
        self.assertEqual(val, val1, val2)

        val = {'bar': 2}
        past = now - 10
        self.tik.set(key, val, timestamp=past, update_index=False)
        val1, t1 = self.tik.get(key)
        val2, t2 = self.tik[key]
        self.assertEqual(now, t1, t2)
        self.assertEqual(val, val1, val2)

        val = {'baz': 3}
        self.tik.set(key, val, timestamp=past, update_index=True)
        val1, t1 = self.tik.get(key)
        val2, t2 = self.tik[key]
        self.assertEqual(past, t1, t2)
        self.assertEqual(val, val1, val2)

        self.tik[key] = val = {'abc': 4}
        val1, t1 = self.tik.get(key)
        val2, t2 = self.tik[key]
        self.assertNotEqual(t1, past, now)
        self.assertEqual(t1, t2)
        self.assertEqual(val, val1, val2)

    def test_remove(self):
        now = int(time.time())
        key = str(uuid.uuid4().hex)[:5]
        val = {'foo': 1}

        self.tik.set(key, val, timestamp=now, update_index=True)
        self.assertNotEqual(len(self.tik), len(self.items))
        self.tik.remove(key)
        self.assertEqual(len(self.tik), len(self.items))

        self.tik.set(key, val, timestamp=now, update_index=True)
        self.assertNotEqual(len(self.tik), len(self.items))
        del self.tik[key]
        self.assertEqual(len(self.tik), len(self.items))

    def test_count(self):
        self.assertEqual(self.tik.count(), len(self.items))
        self.assertEqual(len(self.tik), len(self.items))

    def test_keys(self):
        keys1 = self.tik.keys()
        keys2 = [i[0] for i in self.items]
        self.assertEqual(keys1, keys2)

        keys1 = self.tik.keys(limit=5)
        keys2 = [i[0] for i in self.items[:5]]
        self.assertEqual(keys1, keys2)

        keys1 = self.tik.keys(with_timestamp=True)
        keys2 = [(i[0], i[2]) for i in self.items]
        self.assertEqual(keys1, keys2)

        start_ts, end_ts = self.items[1][2], self.items[4][2]
        keys1 = self.tik.keys(start_ts, end_ts)
        keys2 = [i[0] for i in self.items[1:5]]
        self.assertEqual(keys1, keys2)

        keys1 = self.tik.keys(start_ts, end_ts, limit=2, with_timestamp=True)
        keys2 = [(i[0], i[2]) for i in self.items[1:3]]
        self.assertEqual(keys1, keys2)

    def test_values(self):
        keys = [k for k, _, _ in self.items]
        data1 = self.tik.values(*keys)
        data2 = [(k, v) for k, v, _ in self.items]
        self.assertEqual(data1, data2)

    def test_timerange(self):
        items1 = self.tik.timerange()
        self.assert_ranges_equal(items1, self.items)

        items1 = self.tik.timerange(limit=5)
        self.assert_ranges_equal(items1, self.items[:5])

        start_ts, end_ts = self.items[1][2], self.items[4][2]
        items1 = self.tik.timerange(start_ts, end_ts)
        self.assert_ranges_equal(items1, self.items[1:5])

        items1 = self.tik.timerange(start_ts, end_ts, limit=2)
        self.assert_ranges_equal(items1, self.items[1:3])

        items1 = self.tik.timerange(start_ts, limit=2)
        self.assert_ranges_equal(items1, self.items[1:3])

    def test_count_timerange(self):
        self.assertEqual(self.tik.count_timerange(), len(self.items))

        start_ts, end_ts = self.items[1][2], self.items[4][2]
        count = self.tik.count_timerange(start_ts, end_ts)
        self.assertEqual(count, 4)

        count = self.tik.count_timerange(start_ts)
        self.assertEqual(count, len(self.items) - 1)

    def test_delete_timerange(self):
        start_ts, end_ts = self.items[1][2], self.items[4][2]
        self.tik.delete_timerange(start_ts, end_ts)
        self.assertEqual(len(self.tik), len(self.items) - 4)


class HourIndexedKeyTestCase(TimeIndexedKeyTestCase):
    key_class = keys.HourIndexedKey


class DayIndexedKeyTestCase(TimeIndexedKeyTestCase):
    key_class = keys.DayIndexedKey


class WeekIndexedKeyTestCase(TimeIndexedKeyTestCase):
    key_class = keys.WeekIndexedKey


class MonthIndexedKeyTestCase(TimeIndexedKeyTestCase):
    key_class = keys.MonthIndexedKey


class YaerIndexedKeyTestCase(TimeIndexedKeyTestCase):
    key_class = keys.YearIndexedKey


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import unittest

#from . import base
from . import connections
from . import timelines


client = connections.register_connection()


class TimelineTestCase(unittest.TestCase):

    timeline_class = timelines.Timeline

    def setup_timeline(self):
        self.timeline = self.timeline_class('test')
        self.start_time = int(time.time())
        self.items = []

        for i, ts in enumerate(range(self.start_time, self.start_time + 10)):
            item = {'index': i}
            self.items.append((ts, item))
            self.timeline.record(item, timestamp=ts)

    def teardown_timeline(self):
        self.timeline.delete()

    def assert_ranges_equal(self, items1, items2):
        self.assertEqual(len(items1), len(items2))
        for (ts1, item1), (ts2, item2) in zip(items1, items2):
            self.assertEqual(ts1, ts2)
            self.assertEqual(item1, item2)

    def setUp(self):
        self.setup_timeline()

    def tearDown(self):
        self.teardown_timeline()

    def test_count(self):
        self.assertEqual(self.timeline.count(), len(self.items))
        self.assertEqual(len(self.timeline), len(self.items))

    def test_tail(self):
        ts1, last1 = self.items[-1]
        ts2, last2 = self.timeline.tail()[0]
        self.assertEqual(ts1, ts2)
        self.assertEqual(last1, last2)

        tail1 = self.items[-2:]
        tail2 = self.timeline.tail(2)
        self.assert_ranges_equal(tail1, tail2)

    def test_head(self):
        ts1, first1 = self.items[0]
        ts2, first2 = self.timeline.head()[0]
        self.assertEqual(ts1, ts2)
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
        start_ts, end_ts = start[0], end[0]
        items2 = self.timeline.timerange(start_ts, end_ts)
        self.assert_ranges_equal(items1, items2)

    def test_count_timerange(self):
        items1 = self.items[2:5]
        start, end = self.items[2], self.items[4]
        start_ts, end_ts = start[0], end[0]
        self.assertEqual(len(items1), self.timeline.count_timerange(start_ts, end_ts))

    def test_delete_timerange(self):
        items1 = self.items[:2] + self.items[4:]
        start, end = self.items[2], self.items[3]
        start_ts, end_ts = start[0], end[0]
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


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date, timedelta


def add_month(year, month, delta):
    """
    Helper function which adds `delta` months to current `(year, month)` tuple
    and returns a new valid tuple `(year, month)`
    """
    year, month = divmod(year * 12 + month + delta, 12)
    if month == 0:
        month = 12
        year = year - 1
    return year, month


def not_none(*keys):
    """ Helper function returning first value which is not None. """
    for key in keys:
        if key is not None:
            return key


def iso_year_start(iso_year):
    """ The gregorian calendar date of the first day of the given ISO year. """
    fourth_jan = date(iso_year, 1, 4)
    delta = timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta


def iso_to_gregorian(iso_year, iso_week, iso_day):
    """ Gregorian calendar date for the given ISO year, week and day. """
    year_start = iso_year_start(iso_year)
    return year_start + timedelta(days=iso_day - 1, weeks=iso_week - 1)

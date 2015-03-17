# redis-moment
A powerful analytics python library for Redis.

Installation
------------
The easiest way to install the latest version
is by using pip/easy_install to pull it from PyPI:

    pip install redis-moment

You may also use Git to clone the repository from
Github and install it manually:

    git clone https://github.com/caxap/redis-moment.git
    python setup.py install

Features
--------
1. Advanced data structures optimized for event crunching:
  - Events (Cohort analytics)
  - Counters
  - Timelines
  - Time Indexed Keys
  - Sequences
2. Partitioning by hour, day, week, month and year.
3. Pluggable serialization (default: json, pickle, msgpack)
4. Multiple Redis connections (with aliasing)
5. Key namespacing
6. Local caching (LRU lib requred)
7. Integration with Django

Examples:
```python
from moment import conf, record_events, Sequence, DayEvent

# Register default connection 
conf.register_connection()

# Record some events for user1 and user2
user_ids = Sequence('user_ids')
record_events('user1', ['event1', 'event2'], ['day', 'month'], sequence=user_ids)
record_events('user2', ['event2', 'event3'], ['day', 'month'], sequence=user_ids)

# Inspect some recorded events for today
e1 = DayEvent('event1', sequence=user_ids)
e2 = DayEvent('event2', sequence=user_ids)

assert len(e1) == 1
assert len(e2) == 2
assert 'user1' in e1 and 'user1' in e2
assert 'user2' in e2
assert len(e1 & e2) == 1
assert len(e1 - e2) == 0
```

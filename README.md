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


### Connections

```python
from moment import conf

# Register default connection 
conf.register_connection()

# Register some other connection
conf.register_connection(alias='analytics', host='localhost', port=6379)

analytics_conn = conf.get_connection('analytics')
```

### Sequence

Use Sequence to convert symbolic identifier to sequential ids. Event component uses Sequence under the hood. Also Sequence optionaly holds cache of recenly created ids.  

```python
from moment import Sequence 

users = Sequence('users')
id1 = seq.sequential_id('user1')
id_one = seq.sequential_id('user1')  # will not create new id, and return already assigned value
id2 = seq.sequential_id('user2')
    
assert id1 == 1
assert id1 == id_one
assert id2 == 2
assert 'user1' in users and 'user2' in users
```

### Events

Events makes it possible to implement real-time, highly scalable analytics that can track actions for millions of users in a very little amount of memory. With events you can track active users, user retension, user churn, CTR of user actions and more. You can track events per hour, day, week, month and year. 

```python
from moment import record_events, DayEvent, MonthEvent

# We whant to track active users by day and month. Mark `user1` & `user2` as active. 
record_events(['user1', 'user2'], 'users:active', ['day', 'month'], sequence='users')


# Has `user1` been online today? This month?
active_today = DayEvent('users:active',  sequence='users')
active_this_month = MonthEvent('users:active',  sequence='users')

assert 'user1' in active_today, 'should be active today'
assert 'user1' in active_this_month, 'should be active this month'


# Track some events:
record_events('user1', ['event1', 'event2'], ['day', 'month'], sequence='users')
record_events('user2', ['event2', 'event3'], ['day', 'month'], sequence='users')

# Inspect recorded events for today
e1 = DayEvent('event1', sequence='users')
e2 = DayEvent('event2', sequence='users')

assert len(e1) == 1
assert len(e2) == 2
assert 'user1' in e1 and 'user1' in e2
assert 'user2' in e2
assert len(e1 & e2) == 1
assert len(e1 - e2) == 0
```

#### More docs comming soon...



__all__ = ['LazzyScript', 'monotonic_zadd', 'sequential_id', 'msetbit',
           'multiset_union_update', 'multiset_intersection_update']


class LazzyScript(object):

    def __init__(self, script, client=None):
        self.script = script.read() if hasattr(script, 'read') else script
        self.client = client
        self._func = None

        if client:
            self.load()

    def load(self, client=None, force=False):
        client = client or self.client
        if force or not self._func:
            if not client:
                msg = "Redis client should be given explicitly to call `LazzyScript`."
                raise AssertionError(msg)
            self._func = client.register_script(self.script)

    def __call__(self, keys=[], args=[], client=None):
        client = client or self.client
        if not self._func:
            self.load(client)
        return self._func(keys=keys, args=args, client=client)


monotonic_zadd = LazzyScript("""
    local sequential_id = redis.call('zscore', KEYS[1], ARGV[1])
    if not sequential_id then
        sequential_id = redis.call('zcard', KEYS[1])
        redis.call('zadd', KEYS[1], sequential_id, ARGV[1])
    end
    return sequential_id
""")


def sequential_id(key, identifier, client=None):
    """Map an arbitrary string identifier to a set of sequential ids"""
    return int(monotonic_zadd(keys=[key], args=[identifier], client=client))


msetbit = LazzyScript("""
    for index, value in ipairs(KEYS) do
        redis.call('setbit', value, ARGV[(index - 1) * 2 + 1], ARGV[(index - 1) * 2 + 2])
    end
    return redis.status_reply('ok')
""")


first_key_with_bit_set = LazzyScript("""
    for index, value in ipairs(KEYS) do
        local bit = redis.call('getbit', value, ARGV[1])
        if bit == 1 then
             return value
        end
    end
    return false
""")


multiset_intersection_update = LazzyScript("""
    local keys_values = redis.call('HGETALL', KEYS[1])
    local all = {}
    for i = 1, #keys_values, 2 do
        all[keys_values[i]] = keys_values[i+1]
    end
    redis.call('DEL', KEYS[1])
    for i = 1, #ARGV, 2 do
        local current = tonumber(all[ARGV[i]])
        local new = tonumber(ARGV[i+1])
        if new > 0 and current then
            redis.call('HSET', KEYS[1], ARGV[i], math.min(new, current))
        end
    end
""")


multiset_union_update = LazzyScript("""
    for i = 1, #ARGV, 2 do
        local current = tonumber(redis.call('HGET', KEYS[1], ARGV[i]))
        local new = tonumber(ARGV[i+1])
        if new > 0 and (not current or new > current) then
            redis.call('HSET', KEYS[1], ARGV[i], new)
        end
    end
""")

import functools
from array import array
from rxsci.mux.state import MuxState

class MemoryStore(object):
    """Manages a Memory store

    Statefull MuxObervable operators need to maintain one state per key. Keys
    have to be indexables, i.e. index into a table/array. This allows fast
    lookup (compared to hash tables). As statefull operators are chained in a
    graph, each one builds its own key scheme. In order to unroll these key
    schemes, the key of a MuxObservable item is a nested tuple of keys:

    (k4, (k3, (k2, (k1, (k0)))))

    This class helps in managing muxed items, and store a state for each topmost
    level of the key (i.e. key[0]).
    """
    #__slots__ = 'state', 'keys'

    def __init__(self, name=None, data_type='obj', default_value=None):
        if data_type is int:
            self.create_values = functools.partial(array, 'q')
        elif data_type == 'uint':
            self.create_values = functools.partial(array, 'Q')
        elif data_type is float:
            self.create_values = functools.partial(array, 'd')
        elif data_type is bool:
            self.create_values = functools.partial(array, 'B')
        else:
            self.create_values = list
        self.values = self.create_values()
        self.state = array('B')
        self.default_value = default_value
        self.keys = []

    def add_key(self, key):
        append_count = (key[0]+1) - len(self.state)
        if append_count > 0:
            for _ in range(append_count):
                self.values.append(0)
                self.state.append(MuxState.STATE_CLEARED)
                self.keys.append(MuxState.STATE_CLEARED)
        self.state[key[0]] = MuxState.STATE_NOTSET
        self.keys[key[0]] = key
        if self.default_value is not None:
            print("set value {} on {}".format(self.default_value, key))
            self.set(key, self.default_value)

    def del_key(self, key):
        self.state[key[0]] = MuxState.STATE_CLEARED
        self.keys[key[0]] = MuxState.STATE_CLEARED

    def clear(self):
        self.values = self.create_values()
        self.state = array('B')
        self.keys.clear()

    def is_cleared(self, key):
        if self.state[key[0]] == MuxState.STATE_CLEARED:
            return True
        return False

    def get(self, key):
        #if self.state[key[0]] == MuxState.STATE_CLEARED
        #    return MuxState.STATE_CLEARED
        if self.state[key[0]] == MuxState.STATE_NOTSET:
            return MuxState.STATE_NOTSET
        return self.values[key[0]]

    def set(self, key, value):
        self.keys[key[0]] = key
        self.state[key[0]] = MuxState.STATE_SET
        self.values[key[0]] = value

    def is_set(self, key):
        if self.state[key[0]] == MuxState.STATE_SET:
            return True
        return False

    def iterate(self):
        for index in range(len(self.keys)):
            if self.state[index] is not MuxState.STATE_CLEARED:
                yield (
                    self.keys[index],
                    self.values[index],
                    self.state[index] == MuxState.STATE_SET,
                )

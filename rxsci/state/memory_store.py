import functools
from array import array
import rxsci as rs


def new_index(next_index, free_slots):
    if len(free_slots) > 0:
        index = free_slots.pop()
        return index, next_index, free_slots
    else:
        index = next_index
        return index, next_index+1, free_slots


def del_index(free_slots, index):
    free_slots.append(index)
    return free_slots


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

        self.is_mapper = data_type == 'mapper'
        if self.is_mapper is True:
            self.next_index = 0
            self.free_slots = array('Q')
        self.values = self.create_values()
        self.state = array('B')    
        self.default_value = default_value
        self.data_type = data_type
        self.keys = []

    def add_key(self, key):
        append_count = (key[0]+1) - len(self.state)
        if append_count > 0:
            for _ in range(append_count):
                self.values.append(0)
                self.state.append(rs.state.markers.STATE_CLEARED.value())
                self.keys.append(rs.state.markers.STATE_CLEARED.value())
        self.state[key[0]] = rs.state.markers.STATE_NOTSET.value()
        self.keys[key[0]] = key
        if self.is_mapper:
            self.set(key, {})
        elif self.default_value is not None:
            self.set(key, self.default_value)

    def del_key(self, key):
        self.state[key[0]] = rs.state.markers.STATE_CLEARED.value()
        self.keys[key[0]] = rs.state.markers.STATE_CLEARED.value()

    def clear(self):
        self.values = self.create_values()
        self.state = array('B')
        self.keys.clear()

    def is_cleared(self, key):
        if self.state[key[0]] == rs.state.markers.STATE_CLEARED.value():
            return True
        return False

    def get(self, key):
        #if self.state[key[0]] == rs.state.markers.STATE_CLEARED
        #    return rs.state.markers.STATE_CLEARED
        if self.state[key[0]] == rs.state.markers.STATE_NOTSET.value():
            return rs.state.markers.STATE_NOTSET
        value = self.values[key[0]]
        if self.data_type is bool:
            value = bool(value)
        return value

    def set(self, key, value):
        self.keys[key[0]] = key
        self.state[key[0]] = rs.state.markers.STATE_SET.value()
        self.values[key[0]] = value

    def is_set(self, key):
        if self.state[key[0]] == rs.state.markers.STATE_SET.value():
            return True
        return False

    def iterate(self):
        for index in range(len(self.keys)):
            if self.state[index] is not rs.state.markers.STATE_CLEARED.value():
                yield (
                    self.keys[index],
                    self.values[index],
                    self.state[index] == rs.state.markers.STATE_SET.value(),
                )

    def add_map(self, key, map_key):
        index, self.next_index, self.free_slots = new_index(self.next_index, self.free_slots)
        self.values[key[0]][map_key] = index
        return index

    def get_map(self, key, map_key):
        if not map_key in self.values[key[0]]:
            return rs.state.markers.STATE_NOTSET
        return self.values[key[0]][map_key]

    def del_map(self, key, map_key):
        if not map_key in self.values[key[0]]:
            return rs.state.markers.STATE_NOTSET
        return self.values[key[0]][map_key]

    def iterate_map(self, key):
        for map_key in self.values[key[0]]:
            yield map_key

from collections import namedtuple


ProbeStateTopology = namedtuple('ProbeStateTopology', ['topology'])
ProbeStateTopology.__doc__ = "Event sent to probe for stateful operators"
ProbeStateTopology.topology.__doc__ = "The state topology object to fill"

StateDef = namedtuple('StateDef', ['name', 'data_type', 'default_value'])

class StateTopology(object):
    def __init__(self):
        self.states = []
        self.ids = {}

    def create_mapper(self, name):
        """A mapper is a non-indexable state. Mappers are used in group_by
        operator (where key is mapped to an index). They do not need to be
        stored on persistent storage if no other states are used in the
        applcation.
        """
        return self.create_state(name, data_type='mapper')

    def create_state(self, name, data_type, default_value=None):
        if name in self.ids:
            self.ids[name] += 1
        else:
            self.ids[name] = 0
        unique_name = '{}-{}'.format(name, self.ids[name])
        self.states.append(StateDef(unique_name, data_type, default_value))
        return len(self.states) - 1

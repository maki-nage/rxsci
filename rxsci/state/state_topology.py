from collections import namedtuple


ProbeStateTopology = namedtuple('ProbeStateTopology', ['topology'])
ProbeStateTopology.__doc__ = "Event sent to probe for stateful operators"
ProbeStateTopology.topology.__doc__ = "The state topology object to fill"

StateDef = namedtuple('StateDef', ['name', 'data_type'])

class StateTopology(object):
    def __init__(self):
        self.states = []
        self.mappers = []

    def create_mapper(self, name):
        """A mapper is a non-indexable state. Mappers are used in group_by
        operator (where key is mapped to an index). They do not need to be
        stored on persistent storage if no other states are used in the
        applcation.
        """
        self.mappers.append(name)
        return len(self.mappers) - 1

    def create_state(self, name, data_type):
        print(name)
        print(data_type)
        self.states.append(StateDef(name, data_type))
        return len(self.states) - 1

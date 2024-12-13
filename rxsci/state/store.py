

class Store(object):
    def __init__(self, topology, store_factory):
        """one per partition
        """
        self.states = []
        for state in topology.states:
            self.states.append(store_factory(
                name=state.name,
                data_type=state.data_type,
                default_value=state.default_value,
                global_scope=state.global_scope,
            ))

    def add_key(self, state, key):
        return self.states[state].add_key(key)

    def del_key(self, state, key):
        return self.states[state].del_key(key)

    def set(self, state, key, value):
        return self.states[state].set(key, value)

    def get(self, state, key):
        return self.states[state].get(key)


class StoreManager(object):
    def __init__(self, store_factory):
        """Manages partitions
        """
        self.partitions = None
        self.active_partition = None
        self.topology = None
        self.states = []
        self.create_store = store_factory

    def set_topology(self, topology):
        self.topology = topology

    def get_store(self):
        if self.active_partition is None:
            # No partitioning provided, use a single store
            assert not self.states
            self.states = [Store(topology=self.topology, store_factory=self.create_store)]
            self.active_partition = 0

        return self.states[self.active_partition]

    def add_key(self, state, key):
        store = self.get_store()
        return store.add_key(state, key)

    def del_key(self, state, key):
        store = self.get_store()
        return store.del_key(state, key)

    def set_state(self, state, key, value):
        """Sets value of key in state

        Args:
            state: A state id from topology
            key: A unique key for this state
            value: value to set
        """
        store = self.get_store()
        return store.set(state, key, value)

    def get_state(self, state, key):
        """Retrieves value of key in state

        Args:
            state: A state id from topology
            key: A unique key for this state

        Returns:
            value of key.
        """
        store = self.get_store()
        return store.get(state, key)

    def on_partitions_revoked(self, revoked):
        return

    def on_partitions_assigned(self, assigned):
        return

    def set_active_partition(self, partition):
        self.active_partition = partition

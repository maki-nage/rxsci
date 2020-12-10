import rxsci as rs


def test_set_bool():
    store = rs.state.memory_store.MemoryStore(name='test', data_type=bool)
    store.add_key((0,))
    assert store.get((0,)) is rs.state.markers.STATE_NOTSET
    store.set((0,), False)
    assert store.get((0,)) is False
    store.set((0,), True)
    assert store.get((0,)) is True

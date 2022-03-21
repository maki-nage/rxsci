import pytest
from rxsci.state.state_topology import StateTopology, StateDef


def test_topoplogy():
    tp = StateTopology()

    state = tp.create_state("foo", int)
    assert state == 0
    assert tp.states == [
        StateDef("foo-0", int, None),
    ]

    state = tp.create_state("foo", float)
    assert state == 1
    assert tp.states == [
        StateDef("foo-0", int, None),
        StateDef("foo-1", float, None),
    ]

    state = tp.create_state("bar", float, default_value=1.4)
    assert state == 2
    assert tp.states == [
        StateDef("foo-0", int, None),
        StateDef("foo-1", float, None),
        StateDef("bar-0", float, 1.4),
    ]


def test_topology_create_mapper():
    tp = StateTopology()

    state = tp.create_mapper("foo")
    assert state == 0
    assert tp.states == [
        StateDef("foo-0", 'mapper', None),
    ]


@pytest.mark.skip()
def test_topoplogy_with_state_id():
    tp = StateTopology()

    state = tp.create_state("foo", int)
    assert state == 0
    assert tp.states == [
        StateDef("foo-0", int, None),
    ]

    state = tp.create_state("foo", float, state_id='buz')
    assert state == 1
    assert tp.states == [
        StateDef("foo-0", int, None),
        StateDef("foo-buz", float, None),
    ]

    state = tp.create_state("foo", float, state_id='buz')
    assert state == 1
    assert tp.states == [
        StateDef("foo-0", int, None),
        StateDef("foo-buz", float, None),
    ]

    with pytest.raises(ValueError):
        tp.create_state("foo", int, state_id='buz')

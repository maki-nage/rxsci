import rx
from rx.subject import Subject
import rxsci as rs


def test_with_memory_store():
    s1 = [1, 2, 3, 4]
    s2 = ['a', 'b', 'c', 'd']

    actual_result1 = []
    actual_result2 = []
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)

    ss1, ss2 = rs.state.with_store(store, sources=[
        rx.from_(s1).pipe(rs.ops.mux_observable()),
        rx.from_(s2).pipe(rs.ops.mux_observable()),
    ])

    ss1.subscribe(on_next=actual_result1.append)
    ss2.subscribe(on_next=actual_result2.append)

    assert type(actual_result1[0]) is rs.state.ProbeStateTopology
    assert actual_result1[1:] == [
        rs.OnCreateMux((0,), store),
        rs.OnNextMux((0,), 1, store),
        rs.OnNextMux((0,), 2, store),
        rs.OnNextMux((0,), 3, store),
        rs.OnNextMux((0,), 4, store),
        rs.OnCompletedMux((0,), store),
    ]

    assert type(actual_result2[0]) is rs.state.ProbeStateTopology
    assert actual_result2[1:] == [
        rs.OnCreateMux((0,), store),
        rs.OnNextMux((0,), 'a', store),
        rs.OnNextMux((0,), 'b', store),
        rs.OnNextMux((0,), 'c', store),
        rs.OnNextMux((0,), 'd', store),
        rs.OnCompletedMux((0,), store),
    ]

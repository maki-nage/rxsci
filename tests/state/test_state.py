import rx
import rxsci as rs

from rxsci.state.state_topology import ProbeStateTopology


def on_mux_action(on_next):
    def _on_mux_action(source):
        def on_subscribe(observer, scheduler):
            def _on_next(i):
                on_next(i)
                observer.on_next(i)

            return source.subscribe(
                on_next=_on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )
        return rs.MuxObservable(on_subscribe)

    return _on_mux_action

def test_probe_topology():
    actual_result = []
    actual_store_result = []
    store = rs.state.StoreManager(store_factory=rs.state.MemoryStore)
    rx.from_([1, 2, 3, 4]).pipe(
        rs.ops.multiplex(
            rs.state.with_store(store, rx.pipe(
                on_mux_action(actual_store_result.append)
            ))
        )
    ).subscribe(
        on_next=actual_result.append
    )

    assert actual_result == [1, 2, 3, 4]
    assert type(actual_store_result[0]) == ProbeStateTopology
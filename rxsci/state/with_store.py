import rx
import rxsci as rs

from .state_topology import ProbeStateTopology, StateTopology


def with_store_mux(store, pipeline):
    """
    Args:
        store: AppStore
    """

    def _with_store(source):
        def on_subscribe(observer, scheduler):
            topology = StateTopology()

            def on_next(i):
                observer.on_next(i._replace(store=store))

            observer.on_next(ProbeStateTopology(topology))
            store.set_topology(topology)

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )
        return rs.MuxObservable(on_subscribe)

    return _with_store


def drop_probe_state_topology():
    def _drop_probe_state_topology(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is not ProbeStateTopology:
                    observer.on_next(i._replace(store=None))

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )
        return rs.MuxObservable(on_subscribe)

    return _drop_probe_state_topology


def with_store(store, pipeline):
    '''Use store to manage stateful operations

    Args:
        store: A Store object.
        pipeline: A computation graph where state is stored on store.
    '''
    def _with_store(source):
        if isinstance(source, rs.MuxObservable):
            return rx.pipe(
                with_store_mux(store, pipeline),
                pipeline,
                drop_probe_state_topology(),
            )(source)
        else:
            return rs.ops.multiplex(
                with_store(store, pipeline),
            )(source)

    return _with_store


def with_memory_store(pipeline):
    return with_store(
        rs.state.StoreManager(store_factory=rs.state.MemoryStore),
        pipeline
    )

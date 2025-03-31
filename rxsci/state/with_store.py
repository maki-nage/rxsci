import functools
import rx
from rx.disposable import Disposable
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
                scheduler=scheduler,
            )
        return rs.MuxObservable(on_subscribe)

    return _with_store


class Source(object):
    def __init__(self, source):
        self.source = source
        self.scheduler = None
        self.observer = None
        self.is_completed = False
        self.disposable = None


def with_store_mux_on_sources(sources, store):
    """
    Args:
        store: AppStore
    """
    topology = StateTopology()
    sources = [Source(s) for s in sources]

    def on_next(source, i):
        source.observer.on_next(i._replace(store=store))

    def on_error(source, e):
        source.observer.on_error(e)

    def on_completed(source):
        source.observer.on_completed()

    def dispose_source(source):
        if source.disposable is not None:
            source.disposable.dispose()

    def subscribe_all():
        for source in sources:
            source.disposable = source.source.subscribe(
                on_next=functools.partial(on_next, source),
                on_error=functools.partial(on_error, source),
                on_completed=functools.partial(on_completed, source),
                scheduler=source.scheduler,
            )

    def on_subscribe(index, observer, scheduler):
        if all([s.observer is not None for s in sources]):
            raise RuntimeError("multiple subscriptions in not allowed")

        source = sources[index]
        source.observer = observer
        source.scheduler = scheduler
        observer.on_next(ProbeStateTopology(topology))

        if all([s.observer is not None for s in sources]):
            store.set_topology(topology)
            subscribe_all()

        return Disposable(functools.partial(dispose_source, source))

    return [rs.MuxObservable(functools.partial(
        on_subscribe, index))
        for index, _ in enumerate(sources)
    ]


def drop_probe_state_topology():
    def _drop_probe_state_topology(source):
        def on_subscribe(observer, scheduler):
            def on_next(i):
                if type(i) is not ProbeStateTopology:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler,
            )
        return rs.MuxObservable(on_subscribe)

    return _drop_probe_state_topology


def with_store(store, pipeline=None, sources=None):
    '''Use a state store to manage stateful operations

    Args:
        store: A Store object.
        pipeline: A computation graph where state is stored on store.
    '''
    if pipeline is not None:
        pipeline = rx.pipe(*pipeline) if type(pipeline) is list else pipeline

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
    elif sources is not None:
        return with_store_mux_on_sources(sources, store)
    else:
        raise ValueError("with_store: pipeline or sources must be privided.")

    return _with_store


def with_memory_store(pipeline=None, sources=None):
    '''Use a memory state store to manage stateful operations

    Args:
        pipeline: A computation graph where the state is stored in memory.
    '''
    return with_store(
        rs.state.StoreManager(store_factory=rs.state.MemoryStore),
        pipeline=pipeline,
        sources=sources,
    )

import rxsci as rs
import rx.operators as ops


def take_mux(count):
    def _take(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    value = i.store.get_state(state, i.key)
                    if value > 0:
                        observer.on_next(i)
                        i.store.set_state(state, i.key, value - 1)

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    observer.on_next(i)

                elif type(i) is rs.OnCompletedMux:
                    observer.on_next(i)
                    i.store.del_key(state, i.key)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='take', data_type=int, default_value=count)
                    observer.on_next(i)
                else:
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler
            )

        return rs.MuxObservable(on_subscribe)
    return _take


def take(count):
    """Emits a specified number of items from the start of an
    observable.

    .. marble::
        :alt: take

        -----1--2--3--4----|
        [    take(2)       ]
        -----1--2-|

    Source:
        An Observable or a MuxObservable

    Args:
        count: the number of items to emit

    Returns:
        An observable emitting the first count items from the source
        observable.
    """
    def _take(source):
        if isinstance(source, rs.MuxObservable):
            return take_mux(count)(source)
        else:
            return ops.take(count)(source)

    return _take

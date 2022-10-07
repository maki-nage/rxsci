import rxsci as rs
import rx.operators as ops


def first_mux():
    def _first(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    value = i.store.get_state(state, i.key)
                    if value is False:
                        observer.on_next(i)
                        i.store.set_state(state, i.key, True)

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    observer.on_next(i)

                elif type(i) is rs.OnCompletedMux:
                    observer.on_next(i)
                    i.store.del_key(state, i.key)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='first', data_type=bool, default_value=False)
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
    return _first


def first():
    """Emits the first element of an observable

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: first

        ---1---2---3---4----|
        [      first()      ]
        ---1-|

    Returns:
        An observable emitting the first item from the source
        observable.
    """
    def _first(source):
        if isinstance(source, rs.MuxObservable):
            return first_mux()(source)
        else:
            return ops.first()(source)

    return _first

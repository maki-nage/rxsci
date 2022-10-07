import rxsci as rs
import rx.operators as ops


def last_mux():
    def _last(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    i.store.set_state(state, i.key, i.item)

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    observer.on_next(i)

                elif type(i) is rs.OnCompletedMux:
                    value = i.store.get_state(state, i.key)
                    if value is not rs.state.markers.STATE_NOTSET:
                        observer.on_next(rs.OnNextMux(i.key, value, i.store))
                    observer.on_next(i)
                    i.store.del_key(state, i.key)

                elif type(i) is rs.OnErrorMux:
                    observer.on_next(i)
                    i.store.del_key(state, i.key)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='last', data_type='obj')
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
    return _last


def last():
    """Emits the last element of an observable

    The source can be an Observable or a MuxObservable.

    .. marble::
        :alt: last

        ---1---2---3---4--|
        [      last()     ]
        -----------------4|

    Returns:
        An observable emitting the last item from the source
        observable.
    """
    def _last(source):
        if isinstance(source, rs.MuxObservable):
            return last_mux()(source)
        else:
            return ops.last()(source)

    return _last

import rxsci as rs
import rx.operators as ops


def start_with(padding):
    """Prepends some items to an Observable

    .. marble::
        :alt: start_with

        --1------2--3--4----|
        [start_with((0,10)) ]
        --0-10-1-2--3--4----|

    Source:
        A MuxObservable

    Args:
        mapper: A transform function to invoke with unpacked elements
            as arguments.

    Returns:
        An Observable emitting the items of the source Observable, preceded by
        the values of padding.
    """
    def _start_with(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if type(i) is rs.OnNextMux:
                    s = i.store.get_state(state, i.key)
                    if s is rs.state.markers.STATE_NOTSET:
                        i.store.set_state(state, i.key, True)
                        for p in padding:
                            observer.on_next(i._replace(item=p))
                    observer.on_next(i)

                elif type(i) is rs.OnCreateMux:
                    i.store.add_key(state, i.key)
                    observer.on_next(i)

                elif type(i) in [rs.OnCompletedMux, rs.OnErrorMux]:
                    i.store.del_key(state, i.key)
                    observer.on_next(i)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='start_with', data_type=bool)
                    observer.on_next(i)

                else:
                    observer.on_next(i)


            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
            )

        return rs.MuxObservable(on_subscribe)
    return _start_with

from collections import deque
import rx
import rxsci as rs


def _lag1(source):
    def on_subscribe(observer, scheduler):
        state = None

        def on_next(i):
            nonlocal state
            if type(i) is rs.OnNextMux:
                iprev = i.store.get_state(state, i.key)
                if iprev is rs.state.markers.STATE_NOTSET:
                    iprev = i.item

                ii = (iprev, i.item)
                i.store.set_state(state, i.key, i.item)
                observer.on_next(i._replace(item=ii))
            elif type(i) is rs.OnCreateMux:
                i.store.add_key(state, i.key)
                observer.on_next(i)
            elif type(i) is rs.OnCompletedMux or type(i) is rs.OnErrorMux:
                i.store.del_key(state, i.key)
                observer.on_next(i)
            elif type(i) is rs.state.ProbeStateTopology:
                state = i.topology.create_state(name='lag1', data_type='obj')
                observer.on_next(i)
            else:
                observer.on_next(i)

        return source.subscribe(
            on_next=on_next,
            on_completed=observer.on_completed,
            on_error=observer.on_error,
            scheduler=scheduler)

    return rs.MuxObservable(on_subscribe)


def lag(size=1, data_type='obj'):
    '''Buffers a lag of size on source items

        .. marble::
            :alt: lag1

            -0---1---2-----3----|
            [       lag(1)      ]
            -0,0-0,1-1,2---2,3--|

        .. marble::
            :alt: lag

            -0---1---2-----3----|
            [       lag(2)      ]
            -0,0-0,1-0,2---1,3--|

    Args:
        size: [Optional] size of the lag.
        data_type: [Optional] the type of the lag data.

    Returns:
        An observable where each item is a tuple of (lag, current) items. On
        the first iterations, the item (first, current) is emitted.
    '''
    def _lag(source):
        def on_subscribe(observer, scheduler):
            state = None

            def on_next(i):
                nonlocal state

                if isinstance(i, rs.OnNextMux):
                    q = i.store.get_state(state, i.key)
                    q.append(i.item)
                    observer.on_next(i._replace(item=(q[0], i.item)))
                    if len(q) > size:
                        q.popleft()

                elif isinstance(i, rs.OnCreateMux):
                    i.store.add_key(state, i.key)
                    i.store.set_state(state, i.key, deque())
                    observer.on_next(i)

                elif isinstance(i, rs.OnCompletedMux) \
                or isinstance(i, rs.OnErrorMux):
                    i.store.del_key(state, i.key)
                    observer.on_next(i)

                elif type(i) is rs.state.ProbeStateTopology:
                    state = i.topology.create_state(name='scan', data_type='obj')
                    observer.on_next(i)
                else:
                    observer.on_next(i)

            def on_completed():
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )

        return rs.MuxObservable(on_subscribe)

    if size == 1:
        return _lag1
    return _lag

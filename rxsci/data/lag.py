from collections import deque
import rx
import rxsci as rs
from rxsci.mux.state import MuxState


def _lag1(source):
    def on_subscribe(observer, scheduler):
        last = None

        def on_next(i):
            nonlocal last
            if last is not None:
                observer.on_next((last, i))
            last = i

        return source.subscribe(
            on_next=on_next,
            on_completed=observer.on_completed,
            on_error=observer.on_error,
            scheduler=scheduler)

    def on_subscribe_mux(observer, scheduler):
        state = MuxState()

        def on_next(i):
            if type(i) is rs.OnNextMux:
                ii = (
                    state.get(i.key) if state.is_set(i.key) else i.item,
                    i.item
                )
                state.set(i.key, i.item)
                observer.on_next(rs.OnNextMux(i.key, ii))
            elif type(i) is rs.OnCreateMux:
                state.add_key(i.key)
                observer.on_next(i)
            elif type(i) is rs.OnCompletedMux or type(i) is rs.OnErrorMux:
                state.del_key(i.key)
                observer.on_next(i)
            else:
                observer.on_error(TypeError("lag1: unknow item type: {}".format(type(i))))

        return source.subscribe(
            on_next=on_next,
            on_completed=observer.on_completed,
            on_error=observer.on_error,
            scheduler=scheduler)

    if isinstance(source, rs.MuxObservable):
        return rs.MuxObservable(on_subscribe_mux)
    else:
        return rx.create(on_subscribe)


def lag(size=1):
    '''Buffers a lag of size on source items

        .. marble::
            :alt: lag

            -0--1---2-----3-----|
            [       lag(2)      ]
            --------0,2---1,3---|

    Args:
        size: [Optional] size of the lag.

    Returns:
        An observable where each item is a tuple of (lag, current) items. On
        the first iterations, the item (first, current) is emitted.
    '''
    def _lag(source):
        def on_subscribe(observer, scheduler):
            buffer = deque()

            def on_next(i):
                buffer.append(i)
                observer.on_next((buffer[0], i))
                if len(buffer) > size:
                    buffer.popleft()

            def on_completed():
                buffer.clear()
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )

        def on_subscribe_mux(observer, scheduler):
            buffer = {}

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    q = buffer[i.key]
                    q.append(i.item)
                    observer.on_next(rs.OnNextMux(i.key, (q[0], i.item)))
                    if len(q) > size:
                        q.popleft()

                elif isinstance(i, rs.OnCreateMux):
                    buffer[i.key] = deque()
                    observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux) \
                or isinstance(i, rs.OnErrorMux):
                    del buffer[i.key]
                    observer.on_next(i)

            def on_completed():
                buffer.clear()
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )

        if isinstance(source, rs.MuxObservable):
            return rx.create(on_subscribe_mux)
        else:
            return rx.create(on_subscribe)

    if size == 1:
        return _lag1
    return _lag

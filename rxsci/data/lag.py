import sys
from collections import deque
import rx


def lag1():
    '''Buffers a lag of 1 on source items

        .. marble::
            :alt: lag

            -0--1---2---3---4---|
            [       lag1()      ]
            ----0,1-1,2-2,3-3,4-|

    Returns:
        An observable where each item is a tuple of (lag, current) items. On 
        the first iteration, the item (current, current) is emitted.
    '''
    def _lag1(source):
        def on_subscribe(observer, scheduler):
            last = None

            def on_next(i):
                nonlocal last
                ii = (last if last is not None else i, i)
                last = i
                observer.on_next(ii)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler)
        return rx.create(on_subscribe)

    return _lag1


def lag(size=1):
    '''Buffers a lag of size on source items

        .. marble::
            :alt: lag

            -0--1---2---3---4---|
            [       lag(2)      ]
            --------0,2-1,3-2,4-|

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
        return rx.create(on_subscribe)

    return _lag

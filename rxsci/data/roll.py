from collections import deque

import rx
import rxsci as rs
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, RefCountDisposable
from rx.subject import Subject


def add_ref(xs, r):
    def subscribe(observer, scheduler=None):
        return CompositeDisposable(r.disposable, xs.subscribe(observer))

    return rx.create(subscribe)


def roll(window, step=1, padding=None):
    """Projects each element of an observable sequence into zero or more
    windows which are produced based on element window information.

    .. marble::
        :alt: roll

        -1-2-3-4-5-6-------|
        [      roll(3)     ]
        -+-+-+-+-+-+-+-----|
               +4-5-6|
             +3-4-5|
           +2-3-4|
         +1-2-3|

    Examples:
        >>> rs.data.roll(3),
        >>> rs.data.roll(3, step=2, padding=rs.Padding.RIGHT),

    Args:
        window: Length of each window.
        step: [Optional] Number of elements to step between creation of
            consecutive windows.
        padding: padding method. Use it to pad items left or right on the
            begining and end of the source observable.

    Returns:
        An observable sequence of windows.

    Raises:
        ValueError if window or step is negative
    """

    if window <= 0:
        raise ValueError()

    if step <= 0:
        raise ValueError()

    def _roll(source):
        def subscribe(observer, scheduler=None):
            m = SingleAssignmentDisposable()
            refCountDisposable = RefCountDisposable(m)
            n = [0]
            q = deque()
            last_value = None
            padding_count = int((window) / step)
            if window % step == 0:
                padding_count -= 1

            def create_window():
                s = Subject()
                q.append(s)
                #observer.on_next(add_ref(s, refCountDisposable))
                observer.on_next(s)

            def on_next(x):
                nonlocal last_value
                last_value = x
                if n[0] == 0 and padding == rs.Padding.LEFT:
                    for _ in range(padding_count):
                        create_window()

                    missing_count = padding_count * step
                    for item in q:
                        for _ in range(missing_count):
                            item.on_next(last_value)
                        missing_count -= step

                if (n[0] % step) == 0:
                    create_window()

                for item in q:
                    item.on_next(x)

                c = n[0] - window + 1
                if c % step == 0:
                    if c < 0 and padding == rs.Padding.LEFT:
                        s = q.popleft()
                        s.on_completed()
                    elif c >= 0:
                        s = q.popleft()
                        s.on_completed()

                n[0] += 1

            def on_error(exception):
                while q:
                    q.popleft().on_error(exception)
                observer.on_error(exception)

            def on_completed():
                if padding == rs.Padding.RIGHT:
                    missing_count = (n[0] % step) + 1
                    for item in q:
                        for _ in range(missing_count):
                            item.on_next(last_value)
                        missing_count += step
                    while q:
                        q.popleft().on_completed()
                observer.on_completed()

            m.disposable = source.subscribe_(on_next, on_error, on_completed, scheduler)
            return refCountDisposable
        return rx.create(subscribe)
    return _roll


def roll_buffer(window, step=1, padding=None):
    """Projects each element of an observable sequence into zero or more
    windows which are produced based on element window information.

    .. marble::
        :alt: roll

        -1-2-3-4-5-6-------|
        [      roll(3)     ]
        --1,2,3-2,3,4-3,4,5-4,5,6-|

    Examples:
        >>> rs.data.roll(3),
        >>> rs.data.roll(3, step=2, padding=rs.Padding.RIGHT),

    Args:
        window: Length of each window.
        step: [Optional] Number of elements to step between creation of
            consecutive windows.
        padding: padding method. Use it to pad items left or right on the
            begining and end of the source observable.

    Returns:
        An observable sequence of windows.

    Raises:
        ValueError if window or step is negative
    """

    if window <= 0:
        raise ValueError()

    if step <= 0:
        raise ValueError()

    def _roll(source):
        def subscribe(observer, scheduler=None):
            m = SingleAssignmentDisposable()
            refCountDisposable = RefCountDisposable(m)
            n = [0]
            q = deque()
            last_value = None
            padding_count = int((window) / step)
            if window % step == 0:
                padding_count -= 1

            def create_window():
                s = []
                q.append(s)

            def on_next(x):
                nonlocal last_value
                last_value = x
                if n[0] == 0 and padding == rs.Padding.LEFT:
                    for _ in range(padding_count):
                        create_window()

                    missing_count = padding_count * step
                    for item in q:
                        for _ in range(missing_count):
                            item.append(last_value)
                        missing_count -= step

                if (n[0] % step) == 0:
                    create_window()

                for item in q:
                    item.append(x)

                c = n[0] - window + 1
                if c % step == 0:
                    if c < 0 and padding == rs.Padding.LEFT:
                        s = q.popleft()
                        observer.on_next(s)
                    elif c >= 0:
                        s = q.popleft()
                        observer.on_next(s)

                n[0] += 1

            def on_error(exception):
                observer.on_error(exception)

            def on_completed():
                if padding == rs.Padding.RIGHT:
                    missing_count = (n[0] % step) + 1
                    for item in q:
                        for _ in range(missing_count):
                            item.append(last_value)
                        missing_count += step
                    while q:
                        observer.on_next(q.popleft())
                observer.on_completed()

            m.disposable = source.subscribe_(on_next, on_error, on_completed, scheduler)
            return refCountDisposable
        return rx.create(subscribe)
    return _roll

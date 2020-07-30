from collections import deque

import rx
import rxsci as rs
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, RefCountDisposable
from rx.subject import Subject


def add_ref(xs, r):
    def subscribe(observer, scheduler=None):
        return CompositeDisposable(r.disposable, xs.subscribe(observer))

    return rx.create(subscribe)


def roll(window, stride=None):
    """Projects each element of an observable sequence into zero or more
    windows which are produced based on element window information.

    .. marble::
        :alt: roll

        -1-2-3-4-5-6-------|
        [      roll(3)     ]
        -+-----+-----------|
               +4-5-6|
         +1-2-3|

    Examples:
        >>> rs.data.roll(3),
        >>> rs.data.roll(window=3, step=2),

    Args:
        window: Length of each window.
        stride: [Optional] Number of elements to step between creation of
            consecutive windows. Defaults to window value.

    Returns:
        An observable sequence of windows.

    Raises:
        ValueError if window or step is negative
    """

    if window <= 0:
        raise ValueError()

    if stride is None:
        stride = window
    if stride <= 0:
        raise ValueError()

    def _roll(source):
        def subscribe(observer, scheduler=None):
            state = {}

            def on_next(i):
                if isinstance(i, rs.OnNextMux):
                    istate = state[i.key]
                    w = istate['w']
                    n = istate['n']

                    if (n % stride) == 0 or n == 0:
                        w.append(n)
                        observer.on_next(rs.OnCreateMux((n, i.key)))

                    pop = False
                    for index in w:
                        observer.on_next(rs.OnNextMux((index, i.key), i.item))
                        count = n - index + 1
                        if count == window:                            
                            pop = True
                            observer.on_next(rs.OnCompletedMux((index, i.key)))

                    if pop:
                        w.pop(0)

                    istate['n'] += 1

                elif isinstance(i, rs.OnCreateMux):
                    state[i.key] = {'n': 0, 'w': []}
                    observer.on_next(i)
                elif isinstance(i, rs.OnCompletedMux) \
                or isinstance(i, rs.OnErrorMux):
                    del state[i.key]
                    observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=scheduler)

        return rx.create(subscribe)
    return _roll

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

import rx
from rxsci.internal.utils import NotSet


def with_latest_from(*sources):
    """Merges the specified observables into one observable sequence by
    creating a tuple only when the source observable sequence produces an
    element. The source observable is subscribed once at least one item has
    been received on each additional source.

    .. marble::
        :alt: with_latest_from

          --1--2----3----4-|
        --a-------b---c--d---|
        [ with_latest_from() ]
        ----1,a-2,a-3,b--4,d-|

    Examples:
        >>> rs.ops.with_latest_from(obs1)

    Source:
        An Observable.

    Args:
        sources: Sequence of observables.

    Returns:
        An observable sequence containing the result of combining
        elements of the sources into a tuple.
    """
    def _with_latest_from(parent):
        NO_VALUE = NotSet()

        def on_subscribe(observer, scheduler=None):            
            def subscribe_all(parent, *children):
                parent_subscription = SingleAssignmentDisposable()
                values = [NO_VALUE for _ in children]

                def on_parent_next(value):
                    if NO_VALUE not in values:
                        result = (value,) + tuple(values)
                        observer.on_next(result)

                def subscribe_child(i, child):
                    subscription = SingleAssignmentDisposable()

                    def on_next(value):
                        values[i] = value                        

                        if NO_VALUE not in values and parent_subscription.disposable is None:                            
                            disp = parent.subscribe(on_parent_next, observer.on_error, observer.on_completed, scheduler=scheduler)
                            parent_subscription.disposable = disp                            

                    subscription.disposable = child.subscribe(on_next, observer.on_error, scheduler=scheduler)
                    return subscription            

                children_subscription = [subscribe_child(i, child) for i, child in enumerate(children)]

                return [parent_subscription] + children_subscription
            return CompositeDisposable(subscribe_all(parent, *sources))
        return rx.create(on_subscribe)
    return _with_latest_from

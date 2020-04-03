from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

import rx
from rxsci.internal.utils import NotSet


def with_latest_from(*sources):
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

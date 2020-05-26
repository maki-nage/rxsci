import rx
from rx.subject import Subject


def split(predicate):
    ''' Split an observable based on a predicate criteria.

    Args:
        predicate: A function called for each item, that returns the split 
            criteria.

    Returns:
        A higher order observable returning on observable for each split criteria.
    '''
    def _split(source):
        def on_subscribe(observer, scheduler):
            current_predicate = None
            split_observable = Subject()

            def on_next(i):
                nonlocal current_predicate
                nonlocal split_observable

                new_predicate = predicate(i)
                if current_predicate is None:
                    current_predicate = new_predicate
                    observer.on_next(split_observable)

                if new_predicate != current_predicate:
                    current_predicate = new_predicate
                    split_observable.on_completed()
                    split_observable = Subject()
                    observer.on_next(split_observable)

                split_observable.on_next(i)

            def on_completed():
                split_observable.on_completed()
                observer.on_completed()

            return source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error,
            )
        return rx.create(on_subscribe)
    return _split

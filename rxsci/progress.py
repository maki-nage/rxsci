import rx


def progress(name, threshold):
    '''Prints the progress on item processing

    Prints the number of items that have been processed every threshold items.

    Args:
        name: Name associated to this progress.
        threshold: Period of display for the progress.

    Returns:
        The source observable.
    '''
    def _progress(source):
        def on_subscribe(observer, scheduler):
            counter = 0

            def on_next(i):
                nonlocal counter
                counter += 1
                if counter % threshold == 0:
                    print("{} progress: {}".format(name, counter))
                
                observer.on_next(i)

            return source.subscribe(
                on_next=on_next,
                on_completed=observer.on_completed,
                on_error=observer.on_error,
                scheduler=scheduler,
            )
        return rx.create(on_subscribe)

    return _progress

from timeit import default_timer as timer
import rx


def progress(name, threshold, measure_throughput=True):
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
            countdown = threshold
            prev_time = None

            def on_next(i):
                nonlocal counter
                nonlocal countdown
                nonlocal prev_time
                counter += 1
                countdown -= 1
                if countdown <= 0:
                    countdown = threshold
                    if measure_throughput is True:
                        cur_time = timer()
                        if prev_time is not None:
                            mps = threshold // (cur_time - prev_time)
                        else:
                            mps = None
                        prev_time = cur_time
                        print("{} progress: {} ({} msg/s)".format(name, counter, mps))
                    else:
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

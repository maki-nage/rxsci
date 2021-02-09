from timeit import default_timer as timer
import rx
import rxsci as rs


def progress(name, threshold, measure_throughput=True):
    '''Prints the progress on item processing

    Prints the number of items that have been processed every threshold items.

    The source can be an Observable or a MuxObservable.

    Args:
        name: Name associated to this progress.
        threshold: Period of display for the progress, in unit of item count.

    Returns:
        The source observable.
    '''
    def _progress(acc, i):
        _, counter, countdown, prev_time = acc or (0, threshold, None)

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

        return (i, counter, countdown, prev_time)

    return rx.pipe(
        rs.ops.scan(_progress, seed=None),
        rs.ops.map(lambda i: i[0]),
    )

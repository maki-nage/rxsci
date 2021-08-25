from collections import namedtuple
import rx
import rx.operators as ops
import rxsci as rs
import distogram


def update(bin_count=100, weighted_diff=False, reduce=False):
    '''Updates the distribution by adding source items to it

    The source can be an Observable or a MuxObservable.

    Args:
        bin_count: [Optional] number of bins to use.
        weighted_diff: [Optional] Applies log weight to bin computation. This
            may be needed if the distribution contains outliers.
        reduce: [Optional] Emit an item for each source item when reduce is
            False, otherwise emits a single item on completion.

    Returns:
        An Observable of Distrogram objects.
    '''
    return rx.pipe(
        rs.ops.scan(
            distogram.update,
            seed=lambda: distogram.Distogram(bin_count=bin_count, weighted_diff=weighted_diff),
            reduce=reduce,
    ))


def merge():
    '''Merges distogram distributions.

    The source observable must contain items that are a collection of
    Distogram object. These are typically the result of a zip operation.

    The source can be an Observable or a MuxObservable.

    Returns:
        An Observable of Distogram objects.
    '''
    return rx.pipe(
        rs.ops.map(distogram.merge),
    )


def min():
    '''Returns the minimum value of the distribution.

    The source can be an Observable or a MuxObservable.

    Returns:
        An observable emitting the minimum value of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.bounds(i)[0])
    )


def max():
    '''Returns the maximum value of the distribution.

    The source can be an Observable or a MuxObservable.

    Returns:
        An observable emitting the maximum value of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.bounds(i)[1])
    )


def mean():
    '''Returns the average value of the distribution.

    The source can be an Observable or a MuxObservable.

    Returns:
        An observable emitting the mean value of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.mean(i))
    )


def variance():
    '''Returns the variance value of the distribution.

    The source can be an Observable or a MuxObservable.

    Returns:
        An observable emitting the variance of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.variance(i))
    )


def stddev():
    '''Returns the standard deviation of the distribution.

    The source can be an Observable or a MuxObservable.

    Returns:
        An observable emitting the standard deviation of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.stddev(i))
    )


def quantile(value):
    '''Returns a quantile value of the distribution.

    The source can be an Observable or a MuxObservable.

    Args:
        value: The quantile value to compute, between 0 and 1.

    Returns:
        An observable emitting the quantile value of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.quantile(i, value))
    )


def describe(quantiles=[0.25, 0.5, 0.75]):
    '''Computes statistical metrics of the distribution.

    For each Distogram ditribution received on the source observable, computes
    the following metrics:

    * min
    * max
    * mean
    * standard deviation
    * The quantiles provided as argument

    The source can be an Observable or a MuxObservable.

    Args:
        quantiles: [Optional] A list of quantiles to compute.

    Returns:
        An observable emitting the minimum value of each source items.
    '''
    metrics = [
        min(),
        max(),
        mean(),
        stddev(),
    ]

    fields = ['min', 'max', 'mean', 'stddev']

    for q in quantiles:
        metrics.append(quantile(q))
        fields.append('p{}'.format(int(q*100)))

    x = namedtuple('x', fields)
    return rx.pipe(
        rs.ops.tee_map(*metrics),
        rs.ops.map(lambda i: x(*i)),
    )


def histogram(bin_count=100):
    '''Returns the histogram of the distribution.

    The histogram in in the form of a list of tuples, where each tuple is in
    the form (bin value, element count).

    This operator does not emit items until enough items have been received.

    The source can be an Observable or a MuxObservable.

    Args:
        bin_count: [Optional] Number of bins to use in the histogram.

    Returns:
        An observable emitting the histogram of each source items.
    '''
    return rx.pipe(
        rs.ops.map(lambda i: distogram.histogram(i, bin_count=bin_count)),
        rs.ops.filter(lambda i: i is not None),
    )

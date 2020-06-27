from collections import namedtuple
import rx
import rx.operators as ops
import rxsci as rs
import distogram


def update(weighted_diff=False):
    return rx.pipe(
        ops.scan(
            distogram.update,
            seed=distogram.Distogram(weighted_diff=weighted_diff)
        ))


def merge(weighted_diff=False):
    return rx.pipe(
        ops.scan(
            distogram.merge,
            seed=distogram.Distogram(weighted_diff=weighted_diff)
        ))


def min():
    return rx.pipe(
        ops.map(lambda i: distogram.bounds(i)[0])
    )


def max():
    return rx.pipe(
        ops.map(lambda i: distogram.bounds(i)[1])
    )


def mean():
    return rx.pipe(
        ops.map(lambda i: distogram.mean(i))
    )


def variance():
    return rx.pipe(
        ops.map(lambda i: distogram.variance(i))
    )


def stddev():
    return rx.pipe(
        ops.map(lambda i: distogram.stddev(i))
    )


def quantile(value):
    return rx.pipe(
        ops.map(lambda i: distogram.quantile(i, value))
    )


def describe(quantiles=[0.25, 0.5, 0.75]):
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
        rs.tee_map(*metrics),
        ops.map(lambda i: x(*i)),
    )


def histogram():
    return rx.pipe(
        ops.map(lambda i: distogram.histogram(i))
    )

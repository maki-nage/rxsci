import math
import rx.operators as ops
import rxsci as rs


def stddev(key_mapper=lambda i: i, reduce=False):
    def _stddev(source):

        return source.pipe(
            rs.math.variance(key_mapper, reduce=reduce),
            ops.map(lambda i: math.sqrt(i)),
        )

    return _stddev

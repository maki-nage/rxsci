import rx
from rx import operators as ops
from rx.disposable import Disposable


def ref_count():
    connectable_subscription = None
    count = 0

    def _ref_count(source):
        def subscribe(observer, scheduler=None):
            nonlocal count
            nonlocal connectable_subscription
            count += 1
            should_connect = count == 2
            subscription = source.subscribe(observer, scheduler=scheduler)
            if should_connect:
                connectable_subscription = source.connect(scheduler)

            def dispose():
                nonlocal count
                nonlocal connectable_subscription
                subscription.dispose()
                count -= 1
                if not count:
                    connectable_subscription.dispose()

            return Disposable(dispose)

        return rx.create(subscribe)

    return _ref_count


def train_test_split(test_ratio, sampling_size=1):
    test_modulus = int(1/test_ratio)

    def _train_test_split(source):
        def partition(acc, i):
            if acc is None:
                index = 1
                sampling = sampling_size
            else:
                index = acc[0]
                sampling = acc[1]
            if test_modulus == 0:
                is_test = False
            else:
                is_test = True if index % test_modulus == 0 else False

            sampling -= 1
            if sampling == 0:
                index += 1
                sampling = sampling_size

            return (index, sampling, i, is_test)

        published = source.pipe(
            ops.publish(),
            ref_count()
        )

        return [
            published.pipe(
                ops.scan(partition, seed=None),
                ops.filter(lambda i: i[3] is False),
                ops.map(lambda i: i[2]),
            ),
            published.pipe(
                ops.scan(partition, seed=None),
                ops.filter(lambda i: i[3] is True),
                ops.map(lambda i: i[2]),
            )
        ]

    return _train_test_split

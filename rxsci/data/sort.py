from collections import deque
import rx
import rx.operators as ops
import rxsci as rs


def insert(q, i, key):
    qlen = len(q)
    if qlen == 0:  # empty queue
        q.append(i)
        return q

    partition_size = int(qlen/2)
    low_position = 0
    high_position = partition_size
    ikey = key(i)

    if ikey < key(q[0]):  # insert at head
        q.appendleft(i)
        return q
    elif ikey > key(q[qlen-1]):  # insert at tail
        q.append(i)
        return q
    else:
        while True:
            if ikey > key(q[high_position]):
                low_position = high_position
            else:
                if partition_size == 1:
                    q.insert(low_position + 1, i)
                    return q

            partition_size = max(int(partition_size / 2), 1)
            high_position = min(low_position + partition_size, qlen-1)


def bad_sort(key=lambda i: i):
    def _sort(source):
        def on_subscribe(observer, scheduler):
            acc = deque()

            def on_next(i):
                nonlocal acc
                acc = insert(acc, i, key)

            def on_completed():
                try:
                    while True:
                        observer.on_next(acc.popleft())

                except IndexError:
                    pass

                observer.on_completed()

            source.subscribe(
                on_next=on_next,
                on_completed=on_completed,
                on_error=observer.on_error
            )

        return rx.create(on_subscribe)

    return _sort


def sort(key=lambda i: i):
    '''sort items according to key

    Items are sorted in ascending order.

    Impementation note: This operator caches all the items of the source
    observable before sorting them. It can be used ONLY on BATCH source, and
    consumes a lot of memory.

    Args:
        key: [Optional] function used to extract the sorting key on each item.

    Returns:
        An observable emitting the sorted items of the source observable.
    '''
    def _sort(source):
        return source.pipe(
            ops.to_list(),
            ops.do_action(lambda i: print("loaded dataset")),
            ops.map(lambda i: sorted(i, key=key)),
            ops.do_action(lambda i: print("sorted dataset")),
            rs.data.to_deque(extend=True),
        )

    return _sort

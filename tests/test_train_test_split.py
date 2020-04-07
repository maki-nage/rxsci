import rx
from rx.subject import Subject
import rxsci


def test_tran_test_split():
    dataset = rx.from_([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    train, test = dataset.pipe(
        rxsci.train_test_split(0.2),
    )

    actual_train = []
    actual_test = []

    def on_train_next(i):
        actual_train.append(i)

    def on_test_next(i):
        actual_test.append(i)

    train.subscribe(
        on_next=on_train_next,
        on_error=lambda e: print(e),
    )
    test.subscribe(
        on_next=on_test_next,
        on_error=lambda e: print(e),
    )

    assert actual_train == [0, 1, 2, 3, 5, 6, 7, 8]
    assert actual_test == [4, 9]

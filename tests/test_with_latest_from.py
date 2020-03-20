import rx
from rx.subject import Subject
import rxsci

def test_parent_before_source():
    source = Subject()
    parent = Subject()

    actual_value = None
    def on_next(i):
        nonlocal actual_value
        actual_value = i

    disposable = parent.pipe(
        rxsci.with_latest_from(source),
    ).subscribe(
        on_next=on_next,
        on_error=lambda e: print(e),
    )

    parent.on_next(1)
    assert actual_value == None
    source.on_next('a')
    assert actual_value == None
    parent.on_next(2)
    assert actual_value == (2, 'a')
    parent.on_next(3)
    assert actual_value == (3, 'a')
    actual_value = None
    source.on_next('b')
    assert actual_value == None
    parent.on_next(4)
    assert actual_value == (4, 'b')

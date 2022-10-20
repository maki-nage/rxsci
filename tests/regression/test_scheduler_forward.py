import rx
import rx.operators as ops
from rx.scheduler import NewThreadScheduler
import rxsci as rs


def test_scheduler_forward():
    scheduler = None

    def catch_scheduler(source):
        nonlocal scheduler

        def on_subscribe(observer, _scheduler):
            nonlocal scheduler

            scheduler = _scheduler
            return source.subscribe(
                on_next=observer.on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=_scheduler,
            )

        return rx.create(on_subscribe)

    rx.from_([('a', 1), ('b', 2), ('a', 3), ('b', 4)]).pipe(
        catch_scheduler,
        ops.map(lambda i: i),
        rs.state.with_memory_store(pipeline=[
            rs.ops.group_by(key_mapper=lambda i: i[0], pipeline=[
                rs.ops.tee_map(
                    rs.ops.map(lambda i: i[0]),
                    rs.ops.map(lambda i: i[1]),
                ),
            ]),
            rs.ops.map(lambda i: i),
        ])
    ).run()

    assert type(scheduler) == NewThreadScheduler

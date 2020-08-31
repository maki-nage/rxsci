from collections import namedtuple
import random
import time
import rx
import rx.operators as ops
import rxsci as rs
import rxsci.container as csv

random.seed(42)

Item = namedtuple('Item', ['k1', 'k2', 'k3', 'data'])

dataset_size = 50000000
#dataset_size = 1000000
k1_range = 100000
k2_range = 30
k3_range = 10

def dataset(count):
    for _ in range(count):
        yield Item(
            k1=random.randint(0, k1_range),
            k2=random.randint(0, k2_range),
            k3=random.randint(0, k3_range),
            data=random.random()*1000,
        )

source = dataset(dataset_size)

rx.from_(source).pipe(
    rs.ops.multiplex(rx.pipe(
        rs.ops.group_by(lambda i: i.k1, rx.pipe(
            rs.ops.group_by(lambda i: i.k2, rx.pipe(                        
                rs.data.roll(5, 5, rx.pipe(
                    rs.ops.count(reduce=True),
                )),                
            )),
        )),
    )),
    ops.count(),
).subscribe(
    on_next=lambda i: print(i),
    on_error=lambda e: print(e),
)

print("done!")
time.sleep(5.0)

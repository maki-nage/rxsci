
def _moment(x, c, n):
    m = []
    for i in range(len(x)):
        m.append((x[i]-c)**n)
    return sum(m) / len(x)

from .variance import variance
from .stddev import stddev

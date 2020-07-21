import rxsci as rs


def test_create_aggobservable():
    obs = rs.MuxObservable(lambda o, s: None)

from simulation.common.config import Config, Dist, Func


class TestConfig(Config):
    a = 10
    b = Dist('norm', [100, 10])
    c = Func(lambda x: x)
    d = [Dist('sample', [1, 2, 3, 4]), 2, 1]
    e = {'test': Dist('norm', [50, 10])}


def test_config_fetch():
    t = TestConfig.get()
    assert t.get('a') == 10
    assert t.get('b') > 50
    assert t.get('c')(1) == 1
    assert t.get('d')[0] in [1, 2, 3, 4]
    assert t.get('d')[1] == 2
    assert t.get('e')['test'] > 20
    print(t)

from bami.lz.net_community import get_random_string


def test_ran_string():
    s = get_random_string(10)
    print(s)
    assert len(s) == 10

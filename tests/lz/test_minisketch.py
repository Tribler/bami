from bami.lz.sketch.minisketch import Sketch


def test_minisketch():
    m = Sketch(100)
    for i in range(1, 100):
        m.raw_add(i)
    packet = m.serialize()
    print(len(packet))
    vals = m.decode()
    assert len(vals) == 99

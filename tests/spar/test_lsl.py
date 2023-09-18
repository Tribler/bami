from collections import deque

from bami.spar.payload import UsefulBlobPayload


class LSL:
    def __init__(self, limit):
        self.limit = limit
        self.items = deque(maxlen=limit)

    def append(self, item):
        self.items.append(item)

    def __getitem__(self, index):
        return self.items[index]

    def __len__(self):
        return len(self.items)


def test_lsl():
    lsl = LSL(10)
    for i in range(20):
        lsl.append(i)
    assert len(lsl) == 10
    assert lsl[0] == 10
    assert lsl[9] == 19
    assert  19 in lsl

    val = list(lsl)
    assert val == list(range(10, 20))


def test_lsl2():
    v = UsefulBlobPayload(b'1232', 1)
    lsl = LSL(10)
    lsl.append(v)
    v2 = UsefulBlobPayload(b'1232', 1)

    assert v2 in lsl

from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from typing import List, Optional, Tuple, Union


class Value(object):
    """
    Class for storing DHT values.
    """

    def __init__(
        self, id_: Union[int, bytes], data: bytes, max_age: int, version: int
    ) -> None:
        self.id = id_
        self.data = data
        self.last_update = time.time()
        self.max_age = max_age
        self.version = version

    @property
    def age(self) -> float:
        return time.time() - self.last_update

    @property
    def expired(self) -> bool:
        return self.age > self.max_age

    def __eq__(self, other: Value) -> bool:
        return self.id == other.id

    def __hash__(self):
        return 0


class Storage(object):
    """
    Class for storing key-value pairs in memory.
    """

    def __init__(self) -> None:
        self.items = defaultdict(list)

    def put(
        self,
        key: bytes,
        data: bytes,
        id_: Optional[Union[int, bytes]] = None,
        max_age: int = 86400,
        version: int = 0,
    ) -> None:
        id_ = id_ or hashlib.sha1(data).digest()
        new_value = Value(id_, data, max_age, version)

        try:
            index = self.items[key].index(new_value)
            old_value = self.items[key][index]
            if new_value.version >= old_value.version:
                self.items[key].pop(index)
                self.items[key].insert(0, new_value)
                self.items[key].sort(key=lambda v: 1 if v.id == key else 0)
        except ValueError:
            self.items[key].insert(0, new_value)
            self.items[key].sort(key=lambda v: 1 if v.id == key else 0)

    def get(
        self,
        key: Union[str, bytes],
        starting_point: int = 0,
        limit: Optional[int] = None,
    ) -> List[bytes]:
        upper_bound = (starting_point + limit) if limit else limit
        return (
            [value.data for value in self.items[key][starting_point:upper_bound]]
            if key in self.items
            else []
        )

    def items_older_than(self, min_age: int) -> List[Tuple[bytes, bytes]]:
        items = []
        for key in self.items:
            items += [
                (key, value.data) for value in self.items[key] if value.age > min_age
            ]
        return items

    def clean(self) -> None:
        for key in self.items:
            for index, value in reversed(list(enumerate(self.items[key]))):
                if value.expired:
                    self.items[key].pop(index)
                else:
                    break

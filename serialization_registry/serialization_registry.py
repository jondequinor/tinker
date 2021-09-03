import json
from abc import ABC, abstractmethod
from typing import Any

from pyrsistent import pmap
from pyrsistent.typing import PMap


class _singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class _serializer(ABC):
    @abstractmethod
    def encode(self, obj: Any) -> str:
        raise NotImplementedError("not implemented")

    @abstractmethod
    def decode(self, series: str) -> Any:
        raise NotImplementedError("not implemented")


class _json_serializer(_serializer):
    def encode(self, obj: Any) -> str:
        return json.dumps(obj)

    def decode(self, series: str) -> Any:
        return json.loads(series)


# This singleton is immutable, constant and purely a data source. Should this
# change, i.e. there's a possibility of later registering serializers
# elsewhere, it should cease to be a singleton. So as this stands, serializers
# can be provided elsewhere, but they need to be registered here.
class Registry(metaclass=_singleton):
    def __init__(self) -> None:
        mapping = {
            "application/json": _json_serializer(),
            "application/octet-stream": lambda x: x,
        }
        self._registry: PMap = pmap(mapping)

    def has_serializer(self, mime: str) -> bool:
        return mime in self._registry

    def serializer(self, mime: str) -> _serializer:
        return self._registry.get(mime)


if __name__ == "__main__":
    foo = Registry()
    print(foo.can_serialize("application/json"))
    print(foo.serializer("application/json").encode({"a": 1}))

    bar = foo.serializer("application/json").decode('{"a": 1}')
    print(bar["a"])

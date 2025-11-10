from enum import IntEnum


class SearchAVG(IntEnum):
    VALUE_10 = 10
    VALUE_20 = 20

    def __str__(self) -> str:
        return str(self.value)

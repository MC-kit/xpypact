from typing import Iterable, List

from abc import ABCMeta, abstractmethod
from dataclasses import Field, fields, is_dataclass

import numpy as np

from numpy import ndarray as array


class Extractable(metaclass=ABCMeta):
    @abstractmethod
    def key(self):
        """Extract key value"""

    @abstractmethod
    def properties(self) -> array:
        """Extract data from a dataclass record to numpy array"""

    @classmethod
    def select_sub_indices(cls, *requested_fields: str):

        assert is_dataclass(cls), f"{cls.__name__} is not a dataclass"

        field_names = map(Field.name, fields(cls))

        def iterator() -> array:
            requested_name = next(requested_fields)
            for i, name in enumerate(field_names):
                if requested_name == name:
                    yield i
                    requested_name = next(requested_fields)
            try:
                next(requested_fields)
                raise ValueError(
                    "Not all columns are found. "
                    + "The requested fields should be in the same order as field names."
                )
            except StopIteration:
                pass

        return np.fromiter(iterator(), dtype=np.int32)


def make_extractor(indices=None):
    if not indices:

        def extractor(x: Extractable):
            return x.properties()

    else:

        def extractor(x: Extractable):
            return x.properties()[indices]

    return extractor


default_extractor = total_extractor = make_extractor()


def select_indices_with_arbitrary_order(
    _fields: Iterable[str], *requested_fields: Iterable[str]
) -> List[int]:
    return [i for i, column in enumerate(_fields) if column in requested_fields]

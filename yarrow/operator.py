from abc import ABC, abstractmethod

from pydantic import BaseModel


"""
X.model_json_schema()
result = X.model_validate({...})
    error.errors()
    error.json()
"""


class Operator(ABC):
    @property
    @abstractmethod
    def input(self) -> BaseModel:
        raise NotImplementedError

    @property
    @abstractmethod
    def output(self) -> BaseModel:
        raise NotImplementedError

    ...

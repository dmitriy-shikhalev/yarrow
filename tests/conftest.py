from pydantic import BaseModel
from pytest import fixture

from yarrow.operator import Operator


@fixture()
def model():
    class TestModel(BaseModel):
        a: int

    return TestModel


@fixture()
def operator(model):
    class TestOperator(Operator):
        input = model
        output = model

        @classmethod
        def run(cls, input_: model):
            yield model(a=input_.a * 100)  # type: ignore

    return TestOperator

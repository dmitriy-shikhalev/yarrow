from pydantic import BaseModel

from yarrow.operator import Operator


class Input(BaseModel):
    # pylint: disable=missing-class-docstring
    a: int
    b: int


class Output(BaseModel):
    # pylint: disable=missing-class-docstring
    c: int


class Sum(Operator):
    """
    Calculate sum of two numbers.
    """
    input = Input
    output = Output

    @classmethod
    def run(cls, input_: Input):  # pylint: disable=missing-function-docstring
        yield Output(
            c=input_.a + input_.b
        )


class Mul(Operator):
    """
    Calculate mul of two numbers.
    """
    input = Input
    output = Output

    @classmethod
    def run(cls, input_: Input):  # pylint: disable=missing-function-docstring
        yield Output(
            c=input_.a * input_.b
        )


class Sequence(Operator):
    """
    Return a sequence of numbers from "a" to "b".
    """
    input = Input
    output = Output

    @classmethod
    def run(cls, input_: Input):
        for c in range(input_.a, input_.b):
            yield Output(c=c)


String = 'abc'  # pylint: disable=invalid-name

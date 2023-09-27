from unittest.mock import Mock, patch

import pytest
from pydantic import BaseModel

from yarrow import operator


class TestModel(BaseModel):
    a: int


def f():
    ...


def test_operator_is_abstract_no_input():
    class_ = type('class_', (operator.Operator,), {})

    assert class_.is_abstract is True


def test_operator_is_abstract_no_output():
    class_ = type('class_', (operator.Operator,), {'input': TestModel})

    assert class_.is_abstract is True


def test_operator_is_abstract_no_run():
    class_ = type('class_', (operator.Operator,), {'input': TestModel, 'output': TestModel})

    assert class_.is_abstract is True


def test_operator_is_abstract_ok():
    class_ = type('class_', (operator.Operator,), {'input': TestModel, 'output': TestModel, 'run': f})

    assert class_.is_abstract is False


def test_operator_call_is_abstract():
    with pytest.raises(ValueError):
        operator.Operator.call()


def test_operator_call_ok():
    kwargs = {'a': 3}

    class TestOperator(operator.Operator):
        input = TestModel
        output = TestModel

        @classmethod
        def run(cls, input_: TestModel):
            return TestModel(a=input_.a * 100)

    result = TestOperator.call(**kwargs)

    assert result == {'a': 300}

from unittest.mock import Mock, patch

import pika
import pytest
from pydantic import BaseModel

from yarrow import operator


class TestModel(BaseModel):
    a: int


def f():
    ...


class TestOperator(operator.Operator):
    input = TestModel
    output = TestModel

    @classmethod
    def run(cls, input_: TestModel):
        return TestModel(a=input_.a * 100)


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

    result = TestOperator.call(**kwargs)

    assert result == {'a': 300}


@patch('yarrow.operator.json.dumps')
def test_operator_init(dumps_mock):
    channel = Mock()
    method_frame = Mock()
    properties = Mock()
    body = b'{"a": 3}'

    with patch.object(TestOperator, 'call', Mock()) as call_mock:
        TestOperator(channel, method_frame, properties, body)

        call_mock.assert_called_once_with(a=3)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key=properties.reply_to,
        body=dumps_mock.return_value.encode.return_value,
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        )
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


def test_operator_init_properties_reply_to_none():
    channel = Mock()
    method_frame = Mock()
    properties = Mock(reply_to=None)
    body = b'{"a": 3}'

    TestOperator(channel, method_frame, properties, body)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key='__dead_letters_queue__',
        body=b'{"message": {"a": 3}, "error": "No property reply_to"}'
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


def test_operator_init_method_frame_delivery_tag_none():
    channel = Mock()
    method_frame = Mock(delivery_tag=None)
    properties = Mock()
    body = b'{"a": 3}'

    TestOperator(channel, method_frame, properties, body)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key='__dead_letters_queue__',
        body=b'{"message": {"a": 3}, "error": "No delivery tag"}'
    )


def test_operator_init_properties_correlation_id_none():
    channel = Mock()
    method_frame = Mock()
    properties = Mock(correlation_id=None)
    body = b'{"a": 3}'

    TestOperator(channel, method_frame, properties, body)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key='__dead_letters_queue__',
        body=b'{"message": {"a": 3}, "error": "No correlation_id"}'
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)

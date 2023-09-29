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
        yield TestModel(a=input_.a * 100)


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
        for _ in operator.Operator.call():
            pass


def test_operator_call_ok():
    kwargs = {'a': 3}

    result = TestOperator.call(**kwargs)

    elements = list(result)

    assert len(elements) == 1
    assert elements[0] == {'a': 300}


@patch('yarrow.operator.json.dumps')
def test_operator_init(dumps_mock):
    channel = Mock()
    method_frame = Mock()
    properties = Mock()
    body = b'{"a": 3}'

    with patch.object(TestOperator, 'call', Mock(
        return_value=[{'a': 'b'}]
    )) as call_mock:
        TestOperator(channel, method_frame, properties, body)

        call_mock.assert_called_once_with(a=3)

    assert channel.basic_publish.call_count == 2

    channel.basic_publish.assert_any_call(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":{"a":"b"},"status":"PROCESSING","error":null,"num":0}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":null,"status":"DONE","error":null,"num":1}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        )
    )

    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


@patch('yarrow.operator.json.dumps')
def test_operator_generator(dumps_mock):
    channel = Mock()
    method_frame = Mock()
    properties = Mock()
    body = b'{"a": 3}'

    def generator():
        yield {'a': 'b'}
        yield {'c': 'd'}
        yield {'e': 'f'}

    with patch.object(TestOperator, 'call', Mock(
        return_value=generator()
    )) as call_mock:
        TestOperator(channel, method_frame, properties, body)

        call_mock.assert_called_once_with(a=3)

    assert channel.basic_publish.call_count == 4

    channel.basic_publish.assert_any_call(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":{"a":"b"},"status":"PROCESSING","error":null,"num":0}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":{"c":"d"},"status":"PROCESSING","error":null,"num":1}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":{"e":"f"},"status":"PROCESSING","error":null,"num":2}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":null,"status":"DONE","error":null,"num":3}',
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

    channel.queue_declare.assert_called_once_with('__dead_letters_queue__')
    channel.basic_publish.assert_called_once_with(
        '',
        routing_key='__dead_letters_queue__',
        body=b'{"request":{"a":3},"result":null,"status":"ERROR","error":"No property reply_to","num":0}',
        properties=pika.BasicProperties(correlation_id=properties.correlation_id),
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
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":null,"status":"ERROR","error":"No delivery tag","num":0}',
        properties=pika.BasicProperties(correlation_id=properties.correlation_id),
    )


def test_operator_init_properties_correlation_id_none():
    channel = Mock()
    method_frame = Mock()
    properties = Mock(correlation_id=None)
    body = b'{"a": 3}'

    TestOperator(channel, method_frame, properties, body)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":null,"status":"ERROR","error":"No correlation_id","num":0}',
        properties=pika.BasicProperties(),
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)

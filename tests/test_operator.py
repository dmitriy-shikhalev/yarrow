from unittest.mock import Mock, patch

import pika
import pytest

from yarrow.operator import Operator


def test_operator_is_abstract_no_input():
    class_ = type('class_', (Operator,), {})

    assert class_.is_abstract is True


def test_operator_is_abstract_no_output(model):
    class_ = type('class_', (Operator,), {'input': model})

    assert class_.is_abstract is True


def test_operator_is_abstract_no_run(model):
    class_ = type('class_', (Operator,), {'input': model, 'output': model})

    assert class_.is_abstract is True


def test_operator_is_abstract_ok(model):
    def f():
        ...

    class_ = type('class_', (Operator,), {'input': model, 'output': model, 'run': f})

    assert class_.is_abstract is False


def test_operator_call_is_abstract():
    with pytest.raises(ValueError):
        for _ in Operator.call():
            pass


def test_operator_call_ok(operator):
    kwargs = {'a': 3}

    result = operator.call(**kwargs)

    elements = list(result)

    assert len(elements) == 1
    assert elements[0] == {'a': 300}


def test_operator_init(operator):
    channel = Mock()
    method_frame = Mock()
    properties = Mock(reply_to='a')
    body = b'{"a": 3}'

    with patch.object(operator, 'call', Mock(
        return_value=[{'a': 'b'}]
    )) as call_mock:
        operator(channel, method_frame, properties, body)

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


def test_operator_generator(operator):
    channel = Mock()
    method_frame = Mock()
    properties = Mock(reply_to='a>b')
    body = b'{"a": 3}'

    def generator():
        yield {'a': 'b'}
        yield {'c': 'd'}
        yield {'e': 'f'}

    with patch.object(operator, 'call', Mock(
        return_value=generator()
    )) as call_mock:
        operator(channel, method_frame, properties, body)

        call_mock.assert_called_once_with(a=3)

    assert channel.basic_publish.call_count == 4

    channel.basic_publish.assert_any_call(
        '',
        routing_key='a',
        body=b'{"request":{"a":3},"result":{"a":"b"},"status":"PROCESSING","error":null,"num":0}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
            reply_to='b',
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key='a',
        body=b'{"request":{"a":3},"result":{"c":"d"},"status":"PROCESSING","error":null,"num":1}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
            reply_to='b',
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key='a',
        body=b'{"request":{"a":3},"result":{"e":"f"},"status":"PROCESSING","error":null,"num":2}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
            reply_to='b',
        )
    )
    channel.basic_publish.assert_any_call(
        '',
        routing_key='a',
        body=b'{"request":{"a":3},"result":null,"status":"DONE","error":null,"num":3}',
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id,
            reply_to='b',
        )
    )

    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


def test_operator_init_properties_reply_to_none(operator):
    channel = Mock()
    method_frame = Mock()
    properties = Mock(reply_to=None)
    body = b'{"a": 3}'

    operator(channel, method_frame, properties, body)

    channel.queue_declare.assert_called_once_with('__dead_letters_queue__')
    channel.basic_publish.assert_called_once_with(
        '',
        routing_key='__dead_letters_queue__',
        body=b'{"request":{"a":3},"result":null,"status":"ERROR","error":"No property reply_to","num":0}',
        properties=pika.BasicProperties(correlation_id=properties.correlation_id),
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


def test_operator_init_method_frame_delivery_tag_none(operator):
    channel = Mock()
    method_frame = Mock(delivery_tag=None)
    properties = Mock(reply_to='a')
    body = b'{"a": 3}'

    operator(channel, method_frame, properties, body)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key=properties.reply_to,
        body=b'{"request":{"a":3},"result":null,"status":"ERROR","error":"No delivery tag","num":0}',
        properties=pika.BasicProperties(correlation_id=properties.correlation_id, reply_to=None),
    )


def test_operator_init_properties_correlation_id_none(operator):
    channel = Mock()
    method_frame = Mock()
    properties = Mock(correlation_id=None, reply_to='a>b>c')
    body = b'{"a": 3}'

    operator(channel, method_frame, properties, body)

    channel.basic_publish.assert_called_once_with(
        '',
        routing_key='a',
        body=b'{"request":{"a":3},"result":null,"status":"ERROR","error":"No correlation_id","num":0}',
        properties=pika.BasicProperties(reply_to='b>c'),
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)

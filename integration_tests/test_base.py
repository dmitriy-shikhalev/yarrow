import json
import time
from uuid import uuid4

import pika
import pytest

from settings import Settings


@pytest.fixture
def connection():
    settings = Settings()

    conn = pika.BlockingConnection(
        parameters=pika.ConnectionParameters(
            host=settings.HOST,
            port=settings.PORT,
            virtual_host=settings.VIRTUAL_HOST,
            credentials=pika.PlainCredentials(
                settings.USERNAME,
                settings.PASSWORD,
            )
        )
    )
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def channel(connection):
    channel_ = connection.channel()

    try:
        yield channel_
    finally:
        channel_.close()


@pytest.fixture
def reply_queue(channel):
    QUEUE_NAME = 'reply_queue'
    channel.queue_declare(QUEUE_NAME)

    try:
        yield QUEUE_NAME
    finally:
        channel.queue_delete(QUEUE_NAME)


def test_sum(channel, reply_queue):
    correlation_id = uuid4().hex

    channel.basic_publish(
        '',
        'Sum',
        b'{"a": 100, "b": 1000}',
        properties=pika.BasicProperties(
            reply_to='reply_queue',
            correlation_id=correlation_id
        )
    )

    t = time.time()
    while t - time.time() < 5:
        method_frame, header_frame, body = channel.basic_get('reply_queue', auto_ack=True)

        if body is None:
            time.sleep(0.1)
            continue
        else:
            break
    else:
        raise Exception('Timeout.')

    data = json.loads(body)

    assert data['result'] == {'c': 1100}
    assert data['error'] is None
    assert data['request'] == {'a': 100, 'b': 1000}
    assert data['status'] == 'DONE'
    assert header_frame.correlation_id == correlation_id


def test_sum_error(channel, reply_queue):
    correlation_id = uuid4().hex

    channel.basic_publish(
        '',
        'Sum',
        b'{"a": 100}',
        properties=pika.BasicProperties(
            reply_to='reply_queue',
            correlation_id=correlation_id
        )
    )

    t = time.time()

    while time.time() - t < 5:
        method_frame, header_frame, body = channel.basic_get('reply_queue', auto_ack=True)

        if body is None:
            time.sleep(0.1)
            continue

        break
    else:
        raise Exception('Timeout.')

    data = json.loads(body)
    assert data['result'] is None
    assert data['status'] == 'ERROR'
    assert data['request'] == {'a': 100}
    assert data['error'] is not None
    assert header_frame.correlation_id == correlation_id


def test_sequence(channel, reply_queue):
    correlation_id = uuid4().hex

    channel.basic_publish(
        '',
        'Sequence',
        b'{"a": 100, "b": 110}',
        properties=pika.BasicProperties(
            reply_to='reply_queue',
            correlation_id=correlation_id
        )
    )

    set_ = {num for num in range(100, 110)}

    t0 = time.time()

    while set_ and time.time() - t0 < 10:
        t = time.time()
        while t - time.time() < 5:
            method_frame, header_frame, body = channel.basic_get('reply_queue', auto_ack=True)

            if body is None:
                time.sleep(0.1)
                continue
            else:
                break
        else:
            raise Exception('Timeout.')

        data = json.loads(body)

        if data['status'] == 'PROCESSING':
            assert 'c' in data['result']
            assert data['result']['c'] in set_
            set_.discard(data['result']['c'])
        elif data['status'] == 'DONE':
            assert data['result']['c'] == 110
        assert data['error'] is None
        assert data['request'] == {'a': 100, 'b': 110}
        assert header_frame.correlation_id == correlation_id

    if set_:
        raise ValueError(set_)

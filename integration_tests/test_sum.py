import json
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

    method_frame, header_frame, body = channel.basic_get('reply_queue', auto_ack=True)

    assert json.loads(body) == {'c': 1100}
    assert header_frame.correlation_id == correlation_id


def test_sum_error(channel, reply_queue):
    channel.basic_publish(
        '',
        'Sum',
        b'{"a": 100}',
        properties=pika.BasicProperties(
            reply_to='reply_queue',
            correlation_id=uuid4().hex
        )
    )

    method_frame, header_frame, body = channel.basic_get('__dead_letters_queue__', auto_ack=True)

    assert body == {'c': 1100}

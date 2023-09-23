import json
import logging
from typing import Any, Callable, Type

import pika
from pika.spec import Basic
from pika.channel import Channel
from pydantic import BaseModel


logger = logging.getLogger(__name__)


DEAD_LETTERS_QUEUE = '__dead_letters_queue__'


class Operator:
    # pylint: disable=too-few-public-methods
    """
    Base class for any operators.
    """
    is_abstract: bool = True

    input: Type[BaseModel]
    output: Type[BaseModel]

    run: Callable

    def __init_subclass__(cls: type) -> None:
        """
        Create new Operator class
        """
        input_ = getattr(cls, 'input', None)
        output = getattr(cls, 'output', None)
        if not (
                isinstance(input_, type)
                and isinstance(output, type)
                and issubclass(input_, BaseModel)
                and issubclass(output, BaseModel)
        ):
            logger.warning(
                'Class %s is abstract. input=%s, output=%s',
                cls,
                input_,
                output,
            )
            return

        if not isinstance(getattr(cls, 'run', None), Callable):  # type: ignore
            logger.warning('Class %s is abstract. Not callable method run.', cls)
            return

        cls.is_abstract = False  # type: ignore

    def __init__(self, channel: Channel, method_frame: Basic.GetOk, properties: pika.BasicProperties, body: bytes):
        """
        Init message call.
        """
        logger.info('Start operator %s with body %s', self.__class__.__name__, body)
        try:
            if properties.reply_to is None:
                raise ValueError('No property reply_to')
            if method_frame.delivery_tag is None:
                raise ValueError('No delivery tag')
            if properties.correlation_id is None:
                raise ValueError('No correlation_id')

            result = self.call(**json.loads(body))

            channel.basic_publish(
                '',
                routing_key=properties.reply_to,
                body=json.dumps(result).encode('utf-8'),
                properties=pika.BasicProperties(
                    correlation_id=properties.correlation_id,
                )
            )
            channel.basic_ack(method_frame.delivery_tag)
        except Exception as error:  # pylint: disable=broad-exception-caught
            channel.queue_declare(DEAD_LETTERS_QUEUE)
            channel.basic_publish(
                '',
                routing_key=DEAD_LETTERS_QUEUE,
                body=json.dumps({
                    'message': json.loads(body),
                    'error': str(error),
                }).encode('utf-8')
            )
            if method_frame.delivery_tag is not None:
                channel.basic_ack(method_frame.delivery_tag)

    @classmethod
    def call(cls, **kwargs: Any) -> Any:
        """
        This function is calling.
        """
        if cls.is_abstract:
            raise ValueError(f'Can not use method .call for abstract class {cls}')
        input_ = cls.input.model_validate(kwargs)
        result = cls.run(input_)
        output_ = cls.output.model_validate(result)
        return output_.model_dump()

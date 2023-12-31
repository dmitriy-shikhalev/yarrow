import json
import logging
from importlib import import_module
from typing import Callable

import yaml
from pika import BasicProperties, BlockingConnection, ConnectionParameters, PlainCredentials
from pika.spec import Basic
from pika.adapters.blocking_connection import BlockingChannel

from yarrow.models import OperatorInfo
from yarrow.settings import Settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


OPERATORS = 'operators'
INFO_QUEUE = '__info__'


def read_operator_list() -> list[str]:
    """
    Return all registered operators.
    """
    settings = Settings()

    with open(settings.CONFIG_FILENAME, encoding='utf-8') as file_descriptor:
        generator = yaml.load_all(file_descriptor, Loader=yaml.Loader)
        data = list(generator)

    operators: list[str] = data[0][OPERATORS]
    return operators


def import_operators() -> list[tuple[str, Callable]]:
    """
    Import operators and return pairs: [(name, function), ...].
    """
    operators = read_operator_list()
    operator_pairs: list[tuple[str, Callable]] = []

    for operator in operators:
        try:
            operator_name = operator.rsplit('.', 1)[1]
            package_name = operator.rsplit('.', 1)[0]

            operator_module = import_module(
                package_name,
            )

            function = getattr(
                operator_module,
                operator_name,
            )

            if not callable(function):
                raise ValueError(f'Function {operator_name} in module {package_name} is not callable.')
        except ModuleNotFoundError as error:
            logger.error('Error in %s i: %s', operator, error)
            raise
        operator_pairs.append(
            (
                operator_name,
                function,
            )
        )

    return operator_pairs


def get_info(channel: BlockingChannel, method_frame: Basic.Deliver, properties: BasicProperties, _: bytes) -> None:
    """
    Function return all operators, that can be launched.
    """
    if properties.reply_to is None:
        logger.error('No reply_to')
    else:
        operator_pairs = import_operators()

        operator_info_list = [
            OperatorInfo(
                name=name,
                input=class_.input.model_json_schema(),  # type: ignore
                output=class_.output.model_json_schema(),  # type: ignore
            ) for name, class_ in operator_pairs
        ]

        channel.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=BasicProperties(
                correlation_id=properties.correlation_id,
            ),
            body=json.dumps([info.model_dump() for info in operator_info_list]).encode('utf-8')
        )

    if method_frame.delivery_tag is not None:
        channel.basic_ack(method_frame.delivery_tag)


def serve() -> None:
    """
    Main function: serve and do all business logic of package.
    """
    settings = Settings()
    logger.info(
        'Start serving: %s %s %s %s %s, config file: %s',
        settings.HOST,
        settings.PORT,
        settings.VIRTUAL_HOST,
        settings.USERNAME,
        '***',
        settings.CONFIG_FILENAME,
    )
    operator_pairs = import_operators()
    logger.info('Operators: %s', [
        operator_name
        for operator_name, _ in operator_pairs
    ])
    connection = BlockingConnection(
        parameters=ConnectionParameters(
            host=settings.HOST,
            port=settings.PORT,
            virtual_host=settings.VIRTUAL_HOST,
            credentials=PlainCredentials(
                settings.USERNAME,
                settings.PASSWORD,
            )
        )
    )
    channel = connection.channel()

    try:
        channel.queue_declare(INFO_QUEUE)
        channel.basic_consume(INFO_QUEUE, get_info)

        for operator_name, operator_function in operator_pairs:
            channel.queue_declare(operator_name)

            channel.basic_consume(operator_name, operator_function)

        channel.start_consuming()
    finally:
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()

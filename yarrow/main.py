import logging
from importlib import import_module
from typing import Callable

import pika
import yaml

from yarrow.settings import Settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


ALL_QUEUE = '__all__'
INFO_QUEUE = '__info__'


OPERATORS = 'operators'


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
    connection = pika.BlockingConnection(
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
    channel = connection.channel()

    try:
        for operator_name, operator_function in operator_pairs:
            channel.queue_declare(operator_name)

            channel.basic_consume(operator_name, operator_function)

        channel.start_consuming()
    finally:
        channel.close()
        connection.close()

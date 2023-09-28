import logging
from importlib import import_module

import pika
import yaml

from yarrow.settings import Settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ALL_QUEUE = '__all__'
# INFO_QUEUE = '__info__'


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
    operators = read_operator_list()
    logger.info('Operators: %s', operators)
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
        for operator_qualified_name in operators:
            operator_name = operator_qualified_name.rsplit('.', 1)[1]
            package_name = operator_qualified_name.rsplit('.', 1)[0]

            channel.queue_declare(operator_name)

            operator_module = import_module(
                operator_name,
                package_name,
            )
            channel.basic_consume(operator_name, getattr(
                operator_module,
                operator_name,
            ))

        channel.start_consuming()
    finally:
        channel.close()
        connection.close()

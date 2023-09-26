import logging

import pika

from yarrow.settings import Settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


ALL_QUEUE = '__all__'
INFO_QUEUE = '__info__'


def serve() -> None:
    """
    Main function: serve and do all business logic of package.
    """
    settings = Settings()

    logger.info(
        'Start serving: %s %s %s %s %s',
        settings.HOST,
        settings.PORT,
        settings.VIRTUAL_HOST,
        settings.USERNAME,
        '***',
    )

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
        channel.queue_declare('test')
        # channel.basic_consume('test', test)

        channel.start_consuming()
    finally:
        channel.close()
        connection.close()

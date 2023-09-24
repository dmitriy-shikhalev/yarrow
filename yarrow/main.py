import pika

from yarrow.settings import Settings


def serve() -> None:
    """
    Main function: serve and do all business logic of package.
    """
    settings = Settings()
    connection = pika.connection.Connection(
        parameters=pika.connection.ConnectionParameters(
            host=settings.HOST,
            port=settings.PORT,
            virtual_host=settings.VIRTUAL_HOST,
            credentials=(
                settings.USERNAME,
                settings.PASSWORD,
            )
        )
    )
    raise NotImplementedError(settings)

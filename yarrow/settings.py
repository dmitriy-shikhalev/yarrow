from pydantic import (
    AmqpDsn,
)

from pydantic_settings import BaseSettings  # , SettingsConfigDict


class Settings(BaseSettings):
    # pylint: disable=missing-class-docstring

    RABBITMQ_URI: AmqpDsn
    MODULE_NAME: str

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # pylint: disable=missing-class-docstring

    HOST: str
    PORT: int
    VIRTUAL_HOST: str
    USERNAME: str
    PASSWORD: str

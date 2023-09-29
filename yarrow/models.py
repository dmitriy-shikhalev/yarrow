from enum import Enum
from typing import Any

from pydantic import BaseModel


class Status(Enum):
    DONE = 'DONE'
    ERROR = 'ERROR'


class Answer(BaseModel):
    request: Any
    result: Any | None = None
    status: Status
    error: Any | None = None

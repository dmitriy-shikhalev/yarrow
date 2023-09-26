from typing import Any, Callable

from pydantic import BaseModel


class OperatorMeta(type):
    """
    Meta class for Operator.
    """
    def __new__(cls: type[type], name: str, bases: tuple[type], dct: dict[str, Any]) -> type:
        """
        Create new Operator class
        """

        if not isinstance(dct.get('input'), BaseModel) or not isinstance(dct.get('output'), BaseModel):
            raise ValueError('Attrs input and output should be an instance of pydantic.BaseModel')

        if not isinstance(dct.get('run'), Callable):  # type: ignore
            raise ValueError('Attr run should be a callable')

        class_ = super().__new__(cls, name, bases, dct)  # type: ignore
        return class_  # type: ignore


class Operator(metaclass=OperatorMeta):
    """
    Base class for any operators.
    """
    input: BaseModel
    output: BaseModel

    def call(self, **kwargs: Any) -> Any:
        """
        This function is calling.
        """
        input_ = self.input.model_validate(kwargs)
        result = self.run(input_)
        output_ = self.output.model_validate(result)
        return output_.model_dump()

    def run(self, input_: Any) -> Any:
        """
        Main processing operator function.
        """
        raise NotImplementedError

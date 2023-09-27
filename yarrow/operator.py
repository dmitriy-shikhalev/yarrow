from typing import Any, Callable

from pydantic import BaseModel


class Operator:
    # pylint: disable=too-few-public-methods
    """
    Base class for any operators.
    """
    is_abstract: bool = True

    input: BaseModel
    output: BaseModel

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
            return

        if not isinstance(getattr(cls, 'run', None), Callable):  # type: ignore
            return

        cls.is_abstract = False  # type: ignore

    @classmethod
    def call(cls, **kwargs: Any) -> Any:
        """
        This function is calling.
        """
        if cls.is_abstract:
            raise ValueError('Can not use method .call for abstract class')
        input_ = cls.input.model_validate(kwargs)
        result = cls.run(input_)
        output_ = cls.output.model_validate(result)
        return output_.model_dump()

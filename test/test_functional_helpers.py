from typing import NamedTuple, Optional

import pytest
from functional_helpers import Pipe, AsyncPipe


class CustomException(Exception):
    pass


class PipelineData(NamedTuple):
    name: Optional[str]
    value: Optional[int]


async def get_value_async(param: PipelineData) -> PipelineData:
    return PipelineData(name=param.name, value=1)


async def get_name_async(param: PipelineData) -> PipelineData:
    return PipelineData(name="Bob", value=param.value)


async def bad_func_async(param: PipelineData) -> PipelineData:
    raise CustomException("This is a bad function")


def get_value(param: PipelineData) -> PipelineData:
    return PipelineData(name=param.name, value=1)


def get_name(param: PipelineData) -> PipelineData:
    return PipelineData(name="Bob", value=param.value)


def bad_func(param: PipelineData) -> PipelineData:
    raise CustomException("This is a bad function")


def upper_name(param: PipelineData) -> PipelineData:
    return PipelineData(
        name=param.name.upper() if param.name else None,
        value=param.value,
    )


@pytest.mark.asyncio
class TestAsyncPipe:
    async def test_works_with_async_funcs(self) -> None:
        result = await AsyncPipe(
            get_value_async,
            get_name_async,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="Bob", value=1)

    async def test_works_with_mix_of_async_and_sync(self) -> None:
        result = await AsyncPipe(
            get_value_async,
            get_name_async,
            upper_name,
        )(PipelineData(name=None, value=None))
        assert result == PipelineData(name="BOB", value=1)

    async def test_raises_exception(self) -> None:
        with pytest.raises(CustomException):
            await AsyncPipe(
                get_value_async,
                get_name_async,
                bad_func_async,
            )(PipelineData(name=None, value=None))

    async def test_cannot_pipe_with_no_functions(self) -> None:
        with pytest.raises(AssertionError):
            await AsyncPipe[PipelineData]()(
                PipelineData(name=None, value=None)
            )


@pytest.mark.asyncio
class TestPipe:
    async def test_pipes_correctly(self) -> None:
        result = Pipe(
            get_value,
            get_name,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="Bob", value=1)

    async def test_raises_exception(self) -> None:
        with pytest.raises(CustomException):
            Pipe(
                get_value,
                get_name,
                bad_func,
            )(PipelineData(name=None, value=None))

    async def test_cannot_pipe_with_no_functions(self) -> None:
        with pytest.raises(AssertionError):
            Pipe()(PipelineData(name=None, value=None))  # type: ignore

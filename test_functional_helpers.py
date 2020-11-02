from typing import NamedTuple, Optional

import pytest
from functional_helpers import async_pipe, pipe


class CustomException(Exception):
    pass


class PipelineData(NamedTuple):
    name: Optional[str]
    value: Optional[int]


async def get_value_async(param: PipelineData):
    return PipelineData(name=param.name, value=1)


async def get_name_async(param: PipelineData):
    return PipelineData(name="Bob", value=param.value)


async def bad_func_async(param: PipelineData):
    raise CustomException("This is a bad function")


def get_value(param: PipelineData):
    return PipelineData(name=param.name, value=1)


def get_name(param: PipelineData):
    return PipelineData(name="Bob", value=param.value)


def bad_func(param: PipelineData):
    raise CustomException("This is a bad function")


def upper_name(param: PipelineData):
    return PipelineData(
        name=param.name.upper(),
        value=param.value,
    )


@pytest.mark.asyncio
class TestAsyncPipe:
    async def test_works_with_async_funcs(self):
        result = await async_pipe(
            get_value_async,
            get_name_async,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="Bob", value=1)

    async def test_works_with_mix_of_async_and_sync(self):
        result = await async_pipe(
            get_value_async,
            get_name_async,
            upper_name,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="BOB", value=1)

    async def test_raises_exception(self):
        with pytest.raises(CustomException):
            await async_pipe(
                get_value_async,
                get_name_async,
                bad_func_async,
            )(PipelineData(name=None, value=None))

    async def test_cannot_pipe_with_no_functions(self):
        with pytest.raises(AssertionError):
            await async_pipe()(PipelineData(name=None, value=None))


@pytest.mark.asyncio
class TestPipe:
    async def test_pipes_correctly(self):
        result = pipe(
            get_value,
            get_name,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="Bob", value=1)

    async def test_raises_exception(self):
        with pytest.raises(CustomException):
            await async_pipe(
                get_value,
                get_name,
                bad_func,
            )(PipelineData(name=None, value=None))

    async def test_cannot_pipe_with_no_functions(self):
        with pytest.raises(AssertionError):
            await pipe()(PipelineData(name=None, value=None))

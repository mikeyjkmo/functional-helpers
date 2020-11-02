from typing import NamedTuple, Optional

import pytest
from functional_helpers import async_pipe


class PipelineData(NamedTuple):
    name: Optional[str]
    value: Optional[int]


async def get_value(param: PipelineData):
    return PipelineData(name=param.name, value=1)


async def get_name(param: PipelineData):
    return PipelineData(name="Bob", value=param.value)


def non_async_upper_name(param: PipelineData):
    return PipelineData(
        name=param.name.upper(),
        value=param.value,
    )


@pytest.mark.asyncio
class TestAsyncPipe:
    async def test_works_with_async_funcs(self):
        result = await async_pipe(
            get_value,
            get_name,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="Bob", value=1)

    async def test_works_with_mix_of_async_and_sync(self):
        result = await async_pipe(
            get_value,
            get_name,
            non_async_upper_name,
        )(PipelineData(name=None, value=None))

        assert result == PipelineData(name="BOB", value=1)

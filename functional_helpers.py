from typing import Awaitable, Any, TypeVar, cast, Callable, Generic, Union
from functools import reduce
from inspect import isawaitable


T = TypeVar("T")
D = TypeVar("D")
MaybeAwaitable = Union[D, Awaitable[D]]


async def _ensure_awaitable(res: T) -> T:
    if isawaitable(res):
        return await cast(Awaitable[T], res)
    return res


class AsyncPipe(Generic[D]):
    def __init__(self, *fns: Callable[[D], MaybeAwaitable[D]]):
        assert len(fns) > 1, "There must be at least 2 functions to pipe"
        self.fns = fns

    def __call__(self, initial_input: D) -> Awaitable[D]:
        async def step(acc: Any, f: Any) -> D:
            prev_result = await _ensure_awaitable(acc)
            return await _ensure_awaitable(f(prev_result))
        return reduce(step, self.fns, cast(Awaitable[D], initial_input))


class Pipe(Generic[D]):
    def __init__(self, *fns: Callable[[D], D]):
        assert len(fns) > 1, "There must be at least 2 functions to pipe"
        self.fns = fns

    def __call__(self, initial_input: D) -> D:
        def step(acc: Any, f: Any) -> Any:
            return f(acc)
        return reduce(step, self.fns, initial_input)

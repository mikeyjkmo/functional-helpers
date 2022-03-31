from typing import Awaitable, Any, TypeVar, cast, Callable, Generic, Union
from functools import reduce
from inspect import isawaitable


T = TypeVar("T")
D = TypeVar("D")
E = TypeVar("E", bound=Exception)

Result = Union[T, E]

MaybeAwaitable = Union[D, Awaitable[D]]


async def _ensure_awaitable(res: T) -> T:
    if isawaitable(res):
        return await cast(Awaitable[T], res)
    return res


class AsyncPipe(Generic[D]):
    """
    Pipe async callables together
    """
    def __init__(self, *fns: Callable[[D], MaybeAwaitable[D]]):
        assert len(fns) > 1, "There must be at least 2 functions to pipe"
        self.fns = fns

    def __call__(self, initial_input: D) -> Awaitable[D]:
        async def step(acc: Any, f: Any) -> D:
            prev_result = await _ensure_awaitable(acc)
            return await _ensure_awaitable(f(prev_result))
        return reduce(step, self.fns, cast(Awaitable[D], initial_input))


class Pipe(Generic[D]):
    """
    Pipe callables together
    """
    def __init__(self, *fns: Callable[[D], D]):
        assert len(fns) > 1, "There must be at least 2 functions to pipe"
        self.fns = fns

    def __call__(self, initial_input: D) -> D:
        def step(acc: Any, f: Any) -> Any:
            return f(acc)
        return reduce(step, self.fns, initial_input)


def bind(f: Callable[..., T]) -> Callable[[Result[T, E]], Result[T, E]]:
    """
    Bind a function to allow it to take a result. If
    the result is an error, return it, if not then call the bound function.
    """
    def _bound_f(result: Result[T, E]) -> Result[T, E]:
        if isinstance(result, Exception):
            return cast(E, result)
        return f(result)
    return _bound_f

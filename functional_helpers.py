from functools import reduce, wraps
from inspect import isawaitable


async def _ensure_awaitable(res):
    if isawaitable(res):
        return await res
    return res


def async_pipe(*fns):
    assert len(fns) > 1, "There must be at least 2 functions to pipe"

    @wraps(fns[0])
    def new_fn(initial_input):
        async def step(acc, f):
            prev_result = await _ensure_awaitable(acc)
            return await _ensure_awaitable(f(prev_result))
        return reduce(step, fns, initial_input)

    return new_fn


def pipe(*fns):
    assert len(fns) > 1, "There must be at least 2 functions to pipe"

    @wraps(fns[0])
    def new_fn(initial_input):
        def step(acc, f):
            return f(acc)
        return reduce(step, fns, initial_input)

    return new_fn

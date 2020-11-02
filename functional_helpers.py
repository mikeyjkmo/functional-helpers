from functools import reduce
from inspect import isawaitable


async def _ensure_awaitable(res):
    if isawaitable(res):
        return await res
    return res


def async_pipe(*fns):
    def new_fn(initial_input=None):
        async def step(acc, f):
            prev_result = await _ensure_awaitable(acc)
            return await _ensure_awaitable(f(prev_result))
        return reduce(step, fns, initial_input)
    return new_fn


def pipe(*fns):
    def new_fn(initial_input=None):
        def step(acc, f):
            return f(acc)
        return reduce(step, fns, initial_input)
    return new_fn

import aiohttp
from functools import wraps
from typing import Any, Awaitable, Callable


async def session_async_maker():
    async with aiohttp.ClientSession() as session:
        yield session


def aiohttp_session_decorator(func):
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        async for session in session_async_maker():
            kwargs["session"] = session
            return await func(*args, **kwargs)

    return wrapper


# Пример использования декоратора
@aiohttp_session_decorator
async def fetch_url(url: str, session: aiohttp.ClientSession) -> str:
    async with session.get(url) as response:
        return await response.text()

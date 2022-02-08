import typing as t

import aiohttp

from .utils import LoggerMixin
from .types import HEADERS, PARAMS


class AsyncSession(LoggerMixin):
    def __init__(
        self,
        session: t.Optional[aiohttp.ClientSession] = None,
        timeout: int = 10,
    ) -> None:
        self._session = session
        self._timeout = timeout
        self._logger.info("AsyncSession instantiated")

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._logger.info("ClientSession created")
        return self._session

    async def get(
        self,
        url: str,
        headers: t.Optional[HEADERS] = None,
        params: t.Optional[PARAMS] = None,
        *,
        timeout: int = 10,
    ) -> aiohttp.ClientResponse:
        tout = aiohttp.ClientTimeout(timeout)
        self._logger.info(
            f"Making get request to {url}; "
            f"headers: {headers}; params: {params}"
        )
        response = await self.session.get(
            url=url, headers=headers, params=params, timeout=tout
        )
        await _raise_for_status(response)
        return response

    async def post(
        self,
        url: str,
        headers: HEADERS,
        params: t.Optional[PARAMS] = None,
        data: t.Optional[t.Union[bytes, str]] = None,
        *,
        timeout: int = 10,
    ) -> aiohttp.ClientResponse:
        tout = aiohttp.ClientTimeout(timeout)
        self._logger.info(
            f"Making post request to {url}; "
            f"headers: {headers}; params: {params}"
        )
        response = await self.session.post(
            url=url, headers=headers, params=params, data=data, timeout=tout
        )
        await _raise_for_status(response)
        self._logger.info(f"Request to {url} was successful")
        return response

    async def patch(self):
        raise NotImplementedError

    async def put(self):
        raise NotImplementedError

    async def delete(self):
        raise NotImplementedError

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._logger.info("ClientSession closed")


async def _raise_for_status(response: aiohttp.ClientResponse) -> None:
    if response.status >= 400:
        assert response.reason is not None
        body = await response.text(errors="replace")
        response.release()
        raise aiohttp.ClientResponseError(
            response.request_info,
            response.history,
            status=response.status,
            message=f"{response.reason}: {body}",
            headers=response.headers,
        )

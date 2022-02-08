import typing as t
import datetime
import asyncio
import time
from urllib.parse import urlencode

import aiohttp
import jwt

from .utils import LoggerMixin, get_service_account_data
from .session import AsyncSession
from .exceptions import FailedToReadServiceAccountError


# TODO: Backoff required in case request fails (@backoff)


_GCP_TOKEN_URI = "https://oauth2.googleapis.com/token"
_GCP_TOKEN_DURATION = 3600
_REFRESH_HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}


class BearerToken(LoggerMixin):
    def __init__(
        self,
        service_account_filepath: t.Optional[str] = None,
        session: t.Optional[aiohttp.ClientSession] = None,
        scopes: t.Optional[t.Sequence[str]] = None,
    ) -> None:
        try:
            self._sa_data = get_service_account_data(service_account_filepath)
        except Exception as e:
            raise FailedToReadServiceAccountError(
                f"Failed to access service account content. Error: {e}"
            )
        # self._logger.info(f"SA read successfully. Content: {self._sa_data}")
        self._token_type = self._sa_data["type"]
        self._token_uri = self._sa_data.get("token_uri", _GCP_TOKEN_URI)
        self._logger.info(
            f"Token type: {self._token_type}; Token URI: {self._token_uri}"
        )
        self._session = AsyncSession(session)
        self._scopes = " ".join(scopes or [])

        self._access_token = None
        self._access_token_duration = 0
        self._access_token_acquired_at = datetime.datetime(1970, 1, 1)

        self._acquiring_task: t.Optional[asyncio.Task] = None
        self._logger.info("BearerToken instantiated")

    async def get_token(self) -> t.Optional[str]:
        self._logger.info("Token requested")
        await self._ensure_token_set()
        self._logger.info("Token returned")
        return self._access_token

    async def _ensure_token_set(self) -> None:
        # If acquiring task is already running, await its completion
        if self._acquiring_task is not None:
            await self._acquiring_task
            return

        if not self._access_token:
            self._acquiring_task = asyncio.create_task(
                self._acquire_access_token()
            )
            await self._acquiring_task
            return

        current_time = datetime.datetime.utcnow()
        delta = (current_time - self._access_token_acquired_at).total_seconds()
        if delta <= self._access_token_duration / 2:
            return

        self._logger.info("Acquiring new token...")
        self._acquiring_task = asyncio.create_task(
            self._acquire_access_token()
        )
        await self._acquiring_task
        self._acquiring_task = None  # TODO: This wasnt here but below
        self._logger.info("New token acquired")

    async def _acquire_access_token(self, timeout: int = 10) -> None:
        resp = await self._refresh_service_account(timeout)
        content = await resp.json()
        self._logger.info(f"CONTENT: {content}")
        self._access_token = str(content["access_token"])
        self._access_token_duration = int(content["expires_in"])
        self._access_token_acquired_at = datetime.datetime.utcnow()

    async def _refresh_service_account(
        self, timeout: int = 10
    ) -> aiohttp.ClientResponse:
        token_uri = self._token_uri
        current_time = time.time()
        payload = {
            "aud": token_uri,
            "exp": current_time + _GCP_TOKEN_DURATION,
            "iat": current_time,
            "iss": self._sa_data["client_email"],
            "scope": self._scopes,
        }
        assertion = jwt.encode(
            payload=payload,
            key=self._sa_data["private_key"],
            algorithm="RS256",
        )
        data = urlencode(
            {
                "assertion": assertion,
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            }
        )
        return await self._session.post(
            url=token_uri, headers=_REFRESH_HEADERS, data=data, timeout=timeout
        )

    async def close(self) -> None:
        if self._session:
            await self._session.close()

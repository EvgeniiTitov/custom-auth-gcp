import typing as t
import json

import aiohttp

from auth.token import BearerToken
from auth.session import AsyncSession
from auth.utils import LoggerMixin
from auth.types import HEADERS


_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
_API_ROOT = "https://pubsub.googleapis.com"


class CustomPubSubClient(LoggerMixin):
    def __init__(
        self,
        service_account_filepath: t.Optional[str] = None,
        token: t.Optional[BearerToken] = None,
        session: t.Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._session = AsyncSession(session)
        self._token = token or BearerToken(
            service_account_filepath,
            session=self._session.session,
            scopes=_SCOPES,
        )
        self._logger.info("CustomPubSubClient instantiated")

    async def _get_headers(self) -> HEADERS:
        token = await self._token.get_token()
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

    async def pull_messages(
        self, subscription: str, batch_size: int, *, timeout: int = 30
    ) -> t.List[dict]:
        url = self._get_url(subscription, action="pull")
        headers = await self._get_headers()
        payload = json.dumps({"maxMessages": batch_size}).encode()
        self._logger.info(f"Pulling {batch_size} messages from {subscription}")
        response = await self._session.post(
            url=url, headers=headers, data=payload, timeout=timeout
        )
        response = await response.json()
        self._logger.info(f"Pulled messages from {subscription}")
        messages = response.get("receivedMessages", [])
        return [message for message in messages]

    async def acknowledge_messages(
        self,
        subscription: str,
        acknowledge_ids: t.Sequence[str],
        *,
        timeout: int = 10,
    ) -> None:
        url = self._get_url(subscription, action="acknowledge")
        headers = await self._get_headers()
        payload = json.dumps({"ackIds": acknowledge_ids}).encode()
        self._logger.info(f"Acking ids {acknowledge_ids} from {subscription}")
        await self._session.post(
            url=url, headers=headers, data=payload, timeout=timeout
        )
        self._logger.info(f"Acknowledged ids from {subscription}")

    @staticmethod
    def _get_url(subscription: str, action: str) -> str:
        return f"{_API_ROOT}/v1/{subscription}:{action}"

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self) -> "CustomPubSubClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

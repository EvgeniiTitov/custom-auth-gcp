import logging
import typing as t
import os
import json
import sys


__all__ = ("LoggerMixin", "get_service_account_data")


def get_service_account_data(
    filepath: t.Optional[str] = None,
) -> t.MutableMapping[str, t.Any]:
    path = filepath or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    try:
        with open(path, "r") as file:
            data = json.loads(file.read())
            return data
    except Exception:
        raise


logging.basicConfig(
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
    stream=sys.stdout,
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


class LoggerMixin:
    @property
    def _logger(self) -> logging.Logger:
        name = ".".join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

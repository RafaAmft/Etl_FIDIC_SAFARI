"""
Cliente HTTP com retry automático e backoff exponencial.
"""
import logging
from types import TracebackType
from typing import Optional, Type

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import settings

logger = logging.getLogger(__name__)


class HTTPClient:
    """Cliente HTTP com retry logic."""

    def __init__(self) -> None:
        self.max_retries: int = settings.b3_max_retries
        self._session = requests.Session()
        self._session.headers.update(settings.b3_headers)

    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Faz requisição GET com retry automático e backoff exponencial.

        Args:
            url: URL para requisição.
            **kwargs: Argumentos adicionais para requests.Session.get().

        Returns:
            Response object com status 2xx.

        Raises:
            requests.exceptions.RequestException: Após max_retries tentativas.
        """
        max_retries = self.max_retries

        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(
                (requests.exceptions.RequestException, requests.exceptions.Timeout)
            ),
            reraise=True,
        )
        def _request_with_retry() -> requests.Response:
            logger.debug(f"GET {url[:100]}...")
            response = self._session.get(url, **kwargs)
            response.raise_for_status()
            return response

        return _request_with_retry()

    def close(self) -> None:
        """Fecha a sessão HTTP."""
        self._session.close()

    def __enter__(self) -> "HTTPClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

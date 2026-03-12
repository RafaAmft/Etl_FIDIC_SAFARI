"""
Cliente HTTP com retry automático e backoff exponencial.
"""
import logging
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from src.core.config import api_config

logger = logging.getLogger(__name__)


class HTTPClient:
    """Cliente HTTP com retry logic"""
    
    def __init__(self):
        self.max_retries = api_config.max_retries
        self.headers = api_config.headers
        self._session = requests.Session()
        self._session.headers.update(self.headers)
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Faz requisição GET com retry automático.
        
        O retry é configurado dinamicamente com base em self.max_retries.
        
        Args:
            url: URL para requisição
            **kwargs: Argumentos para requests.get()
            
        Returns:
            Response object
            
        Raises:
            requests.exceptions.RequestException: Após max_retries tentativas
        """
        # Criar retry wrapper dinamicamente para usar config
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((
                requests.exceptions.RequestException,
                requests.exceptions.Timeout
            )),
            reraise=True
        )
        def _request_with_retry():
            logger.debug(f"Requisição: {url[:80]}...")
            response = self._session.get(url, **kwargs)
            response.raise_for_status()
            return response
        
        return _request_with_retry()
    
    def close(self):
        """Fecha a sessão HTTP."""
        self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

"""
API B3 — Wrapper para endpoints da B3 (CVM).
"""
import base64
import binascii
import logging
from typing import Any, Dict, List, Optional

import requests

from config.settings import settings
from src.api.client import HTTPClient
from src.utils.constants import DOCUMENT_LIMIT, DOCUMENT_TYPE

logger = logging.getLogger(__name__)


class B3API:
    """Cliente para API da B3."""

    def __init__(self) -> None:
        self.client = HTTPClient()
        self.url_busca: str = settings.b3_url_busca
        self.url_download: str = settings.b3_url_download
        self.timeout_busca: int = settings.b3_timeout_busca
        self.timeout_download: int = settings.b3_timeout_download

    def buscar_documentos(
        self,
        cnpj: str,
        tipo_documento: str = DOCUMENT_TYPE,
        limite: int = DOCUMENT_LIMIT,
    ) -> List[Dict[str, Any]]:
        """
        Busca documentos de um CNPJ na B3.

        Args:
            cnpj: CNPJ do fundo (14 dígitos).
            tipo_documento: Tipo de documento (filtro local).
            limite: Número máximo de resultados.

        Returns:
            Lista de documentos encontrados (pode estar vazia em caso de erro).
        """
        params = {
            "d": 0,
            "s": 0,
            "l": limite,
            "cnpjFundo": cnpj,
        }

        try:
            response = self.client.get(
                self.url_busca, params=params, timeout=self.timeout_busca
            )
            documentos: List[Dict[str, Any]] = response.json().get("data", [])

            # Filtro local por tipo de documento
            if tipo_documento:
                documentos = [
                    d for d in documentos
                    if d.get("tipoDocumento", "") == tipo_documento
                ]

            logger.debug(f"CNPJ {cnpj}: {len(documentos)} documentos encontrados")
            return documentos

        except requests.exceptions.RequestException as exc:
            logger.error(f"Erro HTTP ao buscar documentos CNPJ {cnpj}: {exc}")
            return []
        except (ValueError, KeyError) as exc:
            logger.error(f"Erro ao parsear resposta da B3 para CNPJ {cnpj}: {exc}")
            return []

    def download_xml(self, doc_id: str) -> Optional[bytes]:
        """
        Baixa conteúdo XML de um documento.

        Args:
            doc_id: ID do documento na B3.

        Returns:
            bytes: Conteúdo XML decodificado, ou None se erro.
        """
        params = {"id": doc_id}

        try:
            response = self.client.get(
                self.url_download, params=params, timeout=self.timeout_download
            )
            # A API B3 retorna o XML codificado em Base64 no body
            xml_content = base64.b64decode(response.content)
            return xml_content

        except binascii.Error as exc:
            logger.error(f"Conteúdo base64 inválido para doc {doc_id}: {exc}")
            return None
        except requests.exceptions.RequestException as exc:
            logger.error(f"Erro HTTP ao baixar XML doc {doc_id}: {exc}")
            return None

"""
API B3 - Wrapper para endpoints da B3 (CVM).
"""
import logging
import base64
from typing import Dict, List, Any, Optional
from src.api.client import HTTPClient
from src.core.config import api_config

logger = logging.getLogger(__name__)


class B3API:
    """Cliente para API da B3"""
    
    def __init__(self):
        self.client = HTTPClient()
        self.url_busca = api_config.url_busca
        self.url_download = api_config.url_download
        self.timeout_busca = api_config.timeout_busca
        self.timeout_download = api_config.timeout_download
    
    def buscar_documentos(
        self,
        cnpj: str,
        tipo_documento: str = "Informe Mensal Estruturado",
        limite: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Busca documentos de um CNPJ na B3.
        
        Args:
            cnpj: CNPJ do fundo (14 dígitos)
            tipo_documento: Tipo de documento a buscar
            limite: Número máximo de resultados
            
        Returns:
            Lista de documentos encontrados
        """
        params = {
            'd': 0,
            's': 0,
            'l': limite,
            'cnpjFundo': cnpj
        }
        
        try:
            response = self.client.get(
                self.url_busca,
                params=params,
                timeout=self.timeout_busca
            )
            data = response.json().get('data', [])
            
            logger.debug(f"CNPJ {cnpj}: {len(data)} documentos encontrados")
            return data
        except Exception as e:
            logger.error(f"Erro ao buscar documentos CNPJ {cnpj}: {e}")
            return []
    
    def download_xml(self, doc_id: str) -> Optional[bytes]:
        """
        Baixa conteúdo XML de um documento.
        
        Args:
            doc_id: ID do documento na B3
            
        Returns:
            bytes: Conteúdo XML decodificado ou None se erro
        """
        params = {'id': doc_id}
        
        try:
            response = self.client.get(
                self.url_download,
                params=params,
                timeout=self.timeout_download
            )
            
            # A API B3 retorna o XML em Base64 diretamente no response.content
            # não em JSON!
            xml_content = base64.b64decode(response.content)
            return xml_content
        except Exception as e:
            logger.error(f"Erro ao baixar XML doc {doc_id}: {e}")
            return None

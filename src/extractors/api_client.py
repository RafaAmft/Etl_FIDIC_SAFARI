"""
Cliente da API B3 para busca e download de documentos FIDC.

Implementa:
- Retry Logic (Resiliência)
- Caching (Performance)
- Logs detalhados

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import requests
import base64
import pandas as pd
from typing import Dict, Optional, Tuple
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import settings
from ..utils.cache_manager import CacheManager


logger = logging.getLogger(__name__)


class B3ApiClient:
    """Cliente para interação com a API da B3 (FNET)."""
    
    def __init__(self):
        """Inicializa o cliente com configurações avançadas."""
        self.url_busca = settings.URL_API_BUSCA
        self.url_download = settings.URL_API_DOWNLOAD
        self.headers = settings.HEADERS
        self.timeout_busca = settings.TIMEOUT_BUSCA
        self.timeout_download = settings.TIMEOUT_DOWNLOAD
        self.limite_docs = settings.LIMITE_DOCS
        
        # Inicializar Cache
        self.cache = CacheManager(cache_dir=".cache_api")
        
        # Configurar Sessão com Retry Strategy
        self.session = self._create_retry_session()
        
    def _create_retry_session(
        self,
        retries=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504)
    ) -> requests.Session:
        """Cria uma sessão requests com estratégia de retry automática."""
        session = requests.Session()
        
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def buscar_documentos(self, cnpj: str) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
        """
        Busca documentos disponíveis para um CNPJ na API B3.
        
        Cache: 24 horas (Metadata de documentos não muda com frequência absurda)
        """
        cache_key = f"docs_{cnpj}"
        cached_data = self.cache.get(cache_key, max_age_seconds=86400) # 24h
        
        if cached_data is not None:
             return True, cached_data, None

        try:
            params = {
                'd': 0,
                's': 0,
                'l': self.limite_docs,
                'cnpjFundo': cnpj
            }
            
            # Usar self.session ao invés de requests.get
            resp = self.session.get(
                self.url_busca,
                params=params,
                headers=self.headers,
                timeout=self.timeout_busca
            )
            resp.raise_for_status()
            
            data = resp.json().get('data', [])
            
            if not data:
                return False, None, "Nenhum documento encontrado"
            
            df_docs = pd.DataFrame(data)
            
            # Salvar no cache
            self.cache.set(cache_key, df_docs)
            
            return True, df_docs, None
            
        except Exception as e:
            logger.error(f"Erro ao buscar documentos {cnpj}: {e}")
            return False, None, f"Erro busca: {str(e)}"
    
    def filtrar_informe_mensal(self, df_docs: pd.DataFrame) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
        """Filtra apenas Informes Mensais ativos."""
        try:
            if df_docs.empty:
                return False, None, "DataFrame vazio"
            
            # Filtrar informe mensal ativo
            # Robustez: Converter para string e tratar NaNs
            df_docs['tipoDocumento'] = df_docs['tipoDocumento'].astype(str)
            df_docs['situacaoDocumento'] = df_docs['situacaoDocumento'].astype(str)
            
            df_mensal = df_docs[
                (df_docs['tipoDocumento'].str.contains("Informe Mensal", case=False, na=False)) &
                (df_docs['situacaoDocumento'].str.strip().str.upper() == "A")
            ].copy()
            
            if df_mensal.empty:
                return False, None, "Nenhum Informe Mensal encontrado"
            
            return True, df_mensal, None
            
        except Exception as e:
            logger.error(f"Erro ao filtrar informe: {e}")
            return False, None, str(e)
    
    def selecionar_documento_mais_recente(self, df_mensal: pd.DataFrame) -> Tuple[bool, Optional[Dict], Optional[str]]:
        """Seleciona o documento com data de referência mais recente."""
        try:
            if df_mensal.empty:
                return False, None, "DataFrame vazio"
            
            # Converter dataReferencia (MM/YYYY ou DD/MM/YYYY) para datetime
            # O formato da B3 geralmente é "mm/yyyy" para informes mensais
            
            # Helper interno para parsing de data flexível
            def parse_data_ref(date_str):
                try:
                    if len(date_str) == 7: # "01/2024"
                        return pd.to_datetime(f"01/{date_str}", format="%d/%m/%Y")
                    return pd.to_datetime(date_str, format="%d/%m/%Y", dayfirst=True)
                except:
                    return pd.NaT

            df_mensal['dataRef_parsed'] = df_mensal['dataReferencia'].astype(str).apply(parse_data_ref)
            
            df_mensal = df_mensal.dropna(subset=['dataRef_parsed'])
            
            if df_mensal.empty:
                return False, None, "Nenhuma data válida encontrada"
            
            # Ordenar por data mais recente
            df_mensal = df_mensal.sort_values('dataRef_parsed', ascending=False)
            doc = df_mensal.iloc[0].to_dict()
            
            return True, doc, None
            
        except Exception as e:
            logger.error(f"Erro ao selecionar documento: {e}")
            return False, None, str(e)
    
    def baixar_xml(self, doc_id: str) -> Tuple[bool, Optional[bytes], Optional[str]]:
        """
        Baixa o XML de um documento específico.
        
        Cache: Indefinido (Conteúdo de documento pelo ID é imutável)
        """
        cache_key = f"xml_{doc_id}"
        cached_content = self.cache.get(cache_key) # Sem limite de idade
        
        if cached_content is not None:
            return True, cached_content, None

        try:
            url_download = f"{self.url_download}?id={doc_id}"
            
            resp = self.session.get(
                url_download,
                headers=self.headers,
                timeout=self.timeout_download
            )
            resp.raise_for_status()
            
            # Decodificar Base64
            # A API retorna o XML codificado em Base64 dentro do body da resposta
            xml_content = base64.b64decode(resp.content)
            
            # Salvar no cache
            self.cache.set(cache_key, xml_content)
            
            return True, xml_content, None
            
        except Exception as e:
            logger.error(f"Erro ao baixar XML {doc_id}: {e}")
            return False, None, f"Erro download: {str(e)}"

"""
Sistema de cache com versionamento para resultados do ETL.

Usa JSON ao invés de pickle para segurança (evita CWE-502 deserialização insegura).
"""
import logging
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from src.core.config import etl_config, paths_config

logger = logging.getLogger(__name__)


class CacheManager:
    """Gerenciador de cache com versionamento (JSON-based)"""
    
    def __init__(self):
        self.cache_enabled = etl_config.cache_enabled
        self.cache_version = etl_config.cache_version
        self.cache_dir = paths_config.cache_dir
    
    def _generate_cache_key(self, cnpj: str, doc_id: str) -> str:
        """Gera chave única para cache usando SHA256."""
        return hashlib.sha256(f"{cnpj}_{doc_id}".encode()).hexdigest()[:16]
    
    def save(self, cnpj: str, doc_id: str, dados: Dict[str, Any]) -> None:
        """
        Salva resultado em cache com versionamento.
        
        Args:
            cnpj: CNPJ do fundo
            doc_id: ID do documento
            dados: Dados a serem salvos
        """
        if not self.cache_enabled:
            return
        
        try:
            cache_key = self._generate_cache_key(cnpj, doc_id)
            cache_file = self.cache_dir / f"{cache_key}.json"
            
            # Adicionar versão aos dados antes de salvar
            dados_com_versao = {'_CACHE_VERSION': self.cache_version, **dados}
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(dados_com_versao, f, ensure_ascii=False, default=str)
            
            logger.debug(
                f"Cache salvo: {cnpj} | doc {doc_id} | versao {self.cache_version}"
            )
        except Exception as e:
            logger.warning(f"Erro ao salvar cache: {e}")
    
    def load(self, cnpj: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Lê resultado do cache se existir e se a versão for compatível.
        
        Args:
            cnpj: CNPJ do fundo
            doc_id: ID do documento
            
        Returns:
            Dict com dados ou None se não encontrado/desatualizado
        """
        if not self.cache_enabled:
            return None
        
        try:
            cache_key = self._generate_cache_key(cnpj, doc_id)
            cache_file = self.cache_dir / f"{cache_key}.json"
            
            # Tentar também formato antigo (.pkl) para migração
            if not cache_file.exists():
                old_pkl = self.cache_dir / f"{cache_key}.pkl"
                if old_pkl.exists():
                    logger.debug(f"Cache antigo (pkl) encontrado, ignorando: {cnpj}")
                    try:
                        old_pkl.unlink()
                    except OSError:
                        pass
                return None
            
            with open(cache_file, 'r', encoding='utf-8') as f:
                dados = json.load(f)
            
            # Verificar versão do cache
            cache_version = dados.get('_CACHE_VERSION', 'v6.0')
            
            if cache_version != self.cache_version:
                logger.debug(
                    f"Cache desatualizado: {cnpj} | doc {doc_id} | "
                    f"versao {cache_version} != {self.cache_version}"
                )
                try:
                    cache_file.unlink()
                    logger.debug(f"Cache desatualizado removido: {cnpj} | doc {doc_id}")
                except OSError:
                    pass
                return None
            
            # Remover metadado de versão antes de retornar
            dados.pop('_CACHE_VERSION', None)
            logger.debug(f"Cache hit: {cnpj} | doc {doc_id} | versao {self.cache_version}")
            return dados
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Erro ao ler cache: {e}")
            return None
    
    def clear_all(self) -> int:
        """
        Limpa todo o cache.
        
        Returns:
            int: Número de arquivos removidos
        """
        count = 0
        try:
            for pattern in ["*.json", "*.pkl"]:
                for cache_file in self.cache_dir.glob(pattern):
                    cache_file.unlink()
                    count += 1
            logger.info(f"Cache limpo: {count} arquivos removidos")
        except Exception as e:
            logger.error(f"Erro ao limpar cache: {e}")
        return count

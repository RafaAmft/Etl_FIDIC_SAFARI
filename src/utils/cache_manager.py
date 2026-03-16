"""
Gerenciador de Cache em Disco.

Permite salvar e recuperar objetos Python (como respostas da API ou DataFrames)
no disco para evitar chamadas repetitivas.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import os
import pickle
import time
import logging
from pathlib import Path
from typing import Any, Optional

from ..config import settings

logger = logging.getLogger(__name__)


class CacheManager:
    """Gerencia cache em disco com expiração (TTL)."""

    def __init__(self, cache_dir: str = ".cache"):
        """
        Inicializa o gerenciador de cache.
        
        Args:
            cache_dir: Diretório onde os arquivos de cache serão salvos.
        """
        self.cache_dir = Path(cache_dir)
        self._ensure_cache_dir()

    def _ensure_cache_dir(self):
        """Cria o diretório de cache se não existir."""
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Não foi possível criar diretório de cache {self.cache_dir}: {e}")

    def _get_file_path(self, key: str) -> Path:
        """Retorna o caminho do arquivo para uma chave."""
        # Sanitizar chave para ser um nome de arquivo válido
        safe_key = "".join([c if c.isalnum() or c in ('-', '_') else '_' for c in key])
        return self.cache_dir / f"{safe_key}.pickle"

    def get(self, key: str, max_age_seconds: Optional[int] = None) -> Optional[Any]:
        """
        Recupera um item do cache.
        
        Args:
            key: Chave única do item.
            max_age_seconds: Idade máxima em segundos. Se None, não expira.
            
        Returns:
            O objeto recuperado ou None se não existir/expirado.
        """
        file_path = self._get_file_path(key)

        if not file_path.exists():
            return None

        try:
            # Verificar expiração
            if max_age_seconds is not None:
                modified_time = file_path.stat().st_mtime
                age = time.time() - modified_time
                if age > max_age_seconds:
                    logger.debug(f"Cache miss (expired): {key} (Age: {age:.1f}s > {max_age_seconds}s)")
                    return None

            # Carregar objeto
            with open(file_path, "rb") as f:
                data = pickle.load(f)
                
            logger.debug(f"Cache hit: {key}")
            return data

        except Exception as e:
            logger.warning(f"Erro ao ler cache {key}: {e}")
            return None

    def set(self, key: str, value: Any):
        """
        Salva um item no cache.
        
        Args:
            key: Chave única do item.
            value: Objeto a ser salvo (deve ser serializável com pickle).
        """
        file_path = self._get_file_path(key)

        try:
            with open(file_path, "wb") as f:
                pickle.dump(value, f)
            logger.debug(f"Cache saved: {key}")
        except Exception as e:
            logger.warning(f"Erro ao salvar cache {key}: {e}")

    def delete(self, key: str):
        """Remove um item do cache."""
        file_path = self._get_file_path(key)
        if file_path.exists():
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"Erro ao excluir cache {key}: {e}")

    def clear_all(self):
        """Limpa todo o cache."""
        try:
            for file in self.cache_dir.glob("*.pickle"):
                os.remove(file)
            logger.info("Cache limpo com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao limpar cache: {e}")

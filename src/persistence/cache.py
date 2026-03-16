"""
Sistema de cache com versionamento para resultados do ETL.

Usa JSON (não pickle) para segurança — evita CWE-502 (deserialização insegura).
Thread-safe via threading.Lock.
"""
import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from config.settings import settings

logger = logging.getLogger(__name__)


class CacheManager:
    """Gerenciador de cache com versionamento JSON-based e thread safety."""

    def __init__(self) -> None:
        self.cache_enabled: bool = settings.etl_cache_enabled
        self.cache_version: str = settings.etl_cache_version
        self.cache_dir: Path = settings.cache_dir
        self._lock = threading.Lock()

    def _generate_cache_key(self, cnpj: str, doc_id: str) -> str:
        """Gera chave única para cache usando SHA256."""
        return hashlib.sha256(f"{cnpj}_{doc_id}".encode()).hexdigest()[:16]

    def save(self, cnpj: str, doc_id: str, dados: Dict[str, Any]) -> None:
        """
        Salva resultado em cache com versionamento.

        Args:
            cnpj: CNPJ do fundo.
            doc_id: ID do documento.
            dados: Dados a serem salvos.
        """
        if not self.cache_enabled:
            return

        with self._lock:
            try:
                cache_key = self._generate_cache_key(cnpj, doc_id)
                cache_file = self.cache_dir / f"{cache_key}.json"
                dados_com_versao = {"_CACHE_VERSION": self.cache_version, **dados}

                with open(cache_file, "w", encoding="utf-8") as fh:
                    json.dump(dados_com_versao, fh, ensure_ascii=False, default=str)

                logger.debug(
                    f"Cache salvo: {cnpj} | doc {doc_id} | versao {self.cache_version}"
                )
            except OSError as exc:
                logger.warning(f"Erro de I/O ao salvar cache ({cnpj}/{doc_id}): {exc}")
            except (TypeError, ValueError) as exc:
                logger.warning(f"Erro de serialização ao salvar cache ({cnpj}/{doc_id}): {exc}")

    def load(self, cnpj: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Lê resultado do cache se existir e se a versão for compatível.

        Args:
            cnpj: CNPJ do fundo.
            doc_id: ID do documento.

        Returns:
            Dict com dados, ou None se não encontrado/desatualizado.
        """
        if not self.cache_enabled:
            return None

        with self._lock:
            try:
                cache_key = self._generate_cache_key(cnpj, doc_id)
                cache_file = self.cache_dir / f"{cache_key}.json"

                # Migração: remover arquivos .pkl legados
                if not cache_file.exists():
                    old_pkl = self.cache_dir / f"{cache_key}.pkl"
                    if old_pkl.exists():
                        logger.debug(f"Cache legado (.pkl) encontrado, removendo: {cnpj}")
                        try:
                            old_pkl.unlink()
                        except OSError:
                            pass
                    return None

                with open(cache_file, "r", encoding="utf-8") as fh:
                    dados: Dict[str, Any] = json.load(fh)

                cached_version = dados.get("_CACHE_VERSION", "unknown")
                if cached_version != self.cache_version:
                    logger.debug(
                        f"Cache desatualizado: {cnpj} | doc {doc_id} | "
                        f"{cached_version!r} != {self.cache_version!r}"
                    )
                    try:
                        cache_file.unlink()
                    except OSError:
                        pass
                    return None

                dados.pop("_CACHE_VERSION", None)
                logger.debug(f"Cache hit: {cnpj} | doc {doc_id}")
                return dados

            except json.JSONDecodeError as exc:
                logger.warning(f"Cache corrompido ({cnpj}/{doc_id}): {exc}")
                return None
            except OSError as exc:
                logger.warning(f"Erro de I/O ao ler cache ({cnpj}/{doc_id}): {exc}")
                return None

    def clear_all(self) -> int:
        """
        Limpa todo o cache.

        Returns:
            int: Número de arquivos removidos.
        """
        count = 0
        with self._lock:
            try:
                for pattern in ("*.json", "*.pkl"):
                    for cache_file in self.cache_dir.glob(pattern):
                        try:
                            cache_file.unlink()
                            count += 1
                        except OSError as exc:
                            logger.warning(f"Não foi possível remover {cache_file}: {exc}")
                logger.info(f"Cache limpo: {count} arquivos removidos")
            except OSError as exc:
                logger.error(f"Erro ao iterar diretório de cache: {exc}")
        return count

"""
Testes para CacheManager.
"""
import json
import threading
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

import pytest

from src.persistence.cache import CacheManager


# ---------------------------------------------------------------------------
# Fixtures e helpers
# ---------------------------------------------------------------------------

SAMPLE_DATA: Dict[str, Any] = {
    "CNPJ_FUNDO": "12345678000195",
    "DATA_COMPETENCIA": "2025-11",
    "ATIVO_TOTAL": 1_000_000.0,
    "NPL_RATIO": 15.0,
}

CNPJ = "12345678000195"
DOC_ID = "doc_001"


def make_cache(tmp_path: Path, enabled: bool = True, version: str = "v8.0") -> CacheManager:
    """Cria um CacheManager apontando para diretório temporário."""
    with (
        patch("src.persistence.cache.settings") as mock_settings
    ):
        mock_settings.etl_cache_enabled = enabled
        mock_settings.etl_cache_version = version
        mock_settings.cache_dir = tmp_path
        cache = CacheManager()
        cache.cache_enabled = enabled
        cache.cache_version = version
        cache.cache_dir = tmp_path
    return cache


# ---------------------------------------------------------------------------
# Testes: save
# ---------------------------------------------------------------------------

class TestCacheSave:

    def test_save_cria_arquivo_json(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        json_files = list(tmp_cache_dir.glob("*.json"))
        assert len(json_files) == 1

    def test_save_conteudo_correto(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        json_file = list(tmp_cache_dir.glob("*.json"))[0]
        with open(json_file, encoding="utf-8") as fh:
            conteudo = json.load(fh)
        assert conteudo["_CACHE_VERSION"] == "v8.0"
        assert conteudo["CNPJ_FUNDO"] == CNPJ
        assert conteudo["NPL_RATIO"] == 15.0

    def test_save_nao_faz_nada_quando_desabilitado(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir, enabled=False)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        assert len(list(tmp_cache_dir.glob("*.json"))) == 0

    def test_save_chave_deterministica(self, tmp_cache_dir: Path) -> None:
        """Salvar duas vezes com mesma chave → mesmo arquivo."""
        cache = make_cache(tmp_cache_dir)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        cache.save(CNPJ, DOC_ID, {**SAMPLE_DATA, "NPL_RATIO": 99.0})
        assert len(list(tmp_cache_dir.glob("*.json"))) == 1


# ---------------------------------------------------------------------------
# Testes: load
# ---------------------------------------------------------------------------

class TestCacheLoad:

    def test_load_retorna_none_quando_desabilitado(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir, enabled=False)
        result = cache.load(CNPJ, DOC_ID)
        assert result is None

    def test_load_retorna_none_quando_nao_existe(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir)
        result = cache.load(CNPJ, "doc_inexistente")
        assert result is None

    def test_load_retorna_dados_apos_save(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        result = cache.load(CNPJ, DOC_ID)
        assert result is not None
        assert result["CNPJ_FUNDO"] == CNPJ
        assert result["NPL_RATIO"] == 15.0

    def test_load_nao_inclui_cache_version(self, tmp_cache_dir: Path) -> None:
        """_CACHE_VERSION deve ser removido antes de retornar."""
        cache = make_cache(tmp_cache_dir)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        result = cache.load(CNPJ, DOC_ID)
        assert result is not None
        assert "_CACHE_VERSION" not in result

    def test_load_retorna_none_versao_errada(self, tmp_cache_dir: Path) -> None:
        """Cache salvo com versão antiga deve ser invalidado."""
        cache_v1 = make_cache(tmp_cache_dir, version="v7.0")
        cache_v1.save(CNPJ, DOC_ID, SAMPLE_DATA)

        cache_v2 = make_cache(tmp_cache_dir, version="v8.0")
        result = cache_v2.load(CNPJ, DOC_ID)
        assert result is None

    def test_load_remove_arquivo_de_versao_errada(self, tmp_cache_dir: Path) -> None:
        """Cache desatualizado deve ser deletado."""
        cache_v1 = make_cache(tmp_cache_dir, version="v7.0")
        cache_v1.save(CNPJ, DOC_ID, SAMPLE_DATA)
        assert len(list(tmp_cache_dir.glob("*.json"))) == 1

        cache_v2 = make_cache(tmp_cache_dir, version="v8.0")
        cache_v2.load(CNPJ, DOC_ID)
        assert len(list(tmp_cache_dir.glob("*.json"))) == 0

    def test_load_retorna_none_com_json_corrompido(self, tmp_cache_dir: Path) -> None:
        """JSON inválido no disco não deve propagar exceção."""
        cache = make_cache(tmp_cache_dir)
        cache_key = cache._generate_cache_key(CNPJ, DOC_ID)
        bad_file = tmp_cache_dir / f"{cache_key}.json"
        bad_file.write_text("{ invalid json ]", encoding="utf-8")

        result = cache.load(CNPJ, DOC_ID)
        assert result is None

    def test_load_remove_pkl_legado(self, tmp_cache_dir: Path) -> None:
        """Arquivos .pkl legados devem ser removidos ao tentar carregar."""
        cache = make_cache(tmp_cache_dir)
        cache_key = cache._generate_cache_key(CNPJ, DOC_ID)
        old_pkl = tmp_cache_dir / f"{cache_key}.pkl"
        old_pkl.write_bytes(b"fake pickle data")

        result = cache.load(CNPJ, DOC_ID)
        assert result is None
        assert not old_pkl.exists()


# ---------------------------------------------------------------------------
# Testes: clear_all
# ---------------------------------------------------------------------------

class TestCacheClearAll:

    def test_clear_remove_todos_os_json(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir)
        cache.save(CNPJ, DOC_ID, SAMPLE_DATA)
        cache.save("98765432000100", "doc_002", SAMPLE_DATA)
        count = cache.clear_all()
        assert count == 2
        assert len(list(tmp_cache_dir.glob("*.json"))) == 0

    def test_clear_remove_pkl_legados(self, tmp_cache_dir: Path) -> None:
        (tmp_cache_dir / "legado.pkl").write_bytes(b"pkl")
        cache = make_cache(tmp_cache_dir)
        count = cache.clear_all()
        assert count == 1
        assert len(list(tmp_cache_dir.glob("*.pkl"))) == 0

    def test_clear_retorna_zero_quando_vazio(self, tmp_cache_dir: Path) -> None:
        cache = make_cache(tmp_cache_dir)
        count = cache.clear_all()
        assert count == 0


# ---------------------------------------------------------------------------
# Testes: thread safety
# ---------------------------------------------------------------------------

class TestCacheThreadSafety:

    def test_save_load_concorrente_sem_corrida(self, tmp_cache_dir: Path) -> None:
        """Múltiplas threads salvando e lendo não devem causar erros."""
        cache = make_cache(tmp_cache_dir)
        errors: list[Exception] = []

        def worker(i: int) -> None:
            try:
                cnpj = f"1234567800{i:04d}"
                doc = f"doc_{i}"
                cache.save(cnpj, doc, {**SAMPLE_DATA, "idx": i})
                result = cache.load(cnpj, doc)
                assert result is not None
                assert result["idx"] == i
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Erros em threads: {errors}"

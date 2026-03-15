"""
Testes de integração para src.backend (contrato Streamlit).

Valida o contrato público das 6 funções do backend:
- get_fundos_resumo
- get_safari_oportunidades
- get_distressed_npl
- get_fundo_historico
- get_estatisticas_gerais
- listar_exports_disponiveis
"""
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pandas as pd
import pytest

from src.backend import (
    get_distressed_npl,
    get_estatisticas_gerais,
    get_fundo_historico,
    get_fundos_resumo,
    get_safari_oportunidades,
    listar_exports_disponiveis,
)


# ---------------------------------------------------------------------------
# Helpers para setup de CSV
# ---------------------------------------------------------------------------

def _create_monitor_csv(
    tmp_path: Path,
    dados: List[Dict[str, Any]],
    filename: str = "fidc_monitor_completo_20250115_120000.csv",
) -> Path:
    """Cria CSV de monitor em subdiretório datado (como ETL produz)."""
    subdir = tmp_path / "2025-01-15"
    subdir.mkdir(parents=True, exist_ok=True)
    csv_path = subdir / filename
    df = pd.DataFrame(dados)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path


def _patch_output_dir(tmp_path: Path):
    """Context manager que faz settings.output_dir apontar para tmp_path."""
    return patch("src.backend.settings")


# ---------------------------------------------------------------------------
# Fixture: dados mínimos para todos os testes
# ---------------------------------------------------------------------------

@pytest.fixture
def dados_monitor() -> List[Dict[str, Any]]:
    return [
        {
            "CNPJ_FUNDO": "31405473000100",
            "DATA_COMPETENCIA": "2025-10",
            "ATIVO_TOTAL": 91_630_153.0,
            "PATRIMONIO_LIQUIDO": 70_000_000.0,
            "NPL_RATIO": 5.0,
            "ZUMBI_RATIO": 2.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 0.8,
            "SCORE_SAFARI": 15.0,
            "CLASSIFICACAO_SAFARI": "BAIXA_PRIORIDADE",
            "STATUS_DADOS": "VALIDADO",
            "ANOMALIA_ESTRUTURAL": False,
        },
        {
            "CNPJ_FUNDO": "12345678000195",
            "DATA_COMPETENCIA": "2025-10",
            "ATIVO_TOTAL": 50_000_000.0,
            "PATRIMONIO_LIQUIDO": 35_000_000.0,
            "NPL_RATIO": 30.0,
            "ZUMBI_RATIO": 12.5,
            "LIQUIDEZ_IMEDIATA_RATIO": 0.25,
            "SCORE_SAFARI": 45.5,
            "CLASSIFICACAO_SAFARI": "MONITORAR",
            "STATUS_DADOS": "VALIDADO",
            "ANOMALIA_ESTRUTURAL": False,
        },
        {
            "CNPJ_FUNDO": "99999999000191",
            "DATA_COMPETENCIA": "2025-09",
            "ATIVO_TOTAL": 1_000.0,
            "PATRIMONIO_LIQUIDO": 0.0,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 0.0,
            "SCORE_SAFARI": 0.0,
            "CLASSIFICACAO_SAFARI": "DADOS_INVALIDOS",
            "STATUS_DADOS": "CARTEIRA_ZERADA",
            "ANOMALIA_ESTRUTURAL": True,
        },
    ]


# ---------------------------------------------------------------------------
# Testes: get_fundos_resumo
# ---------------------------------------------------------------------------

class TestGetFundosResumo:

    def test_retorna_dataframe_quando_csv_existe(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundos_resumo()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_retorna_dataframe_vazio_sem_csv(self, tmp_path: Path) -> None:
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path / "vazio"
            df = get_fundos_resumo()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_colunas_garantidas(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundos_resumo()
        colunas_esperadas = [
            "CNPJ_FUNDO", "DATA_COMPETENCIA", "ATIVO_TOTAL", "NPL_RATIO",
            "SCORE_SAFARI", "CLASSIFICACAO_SAFARI",
        ]
        for col in colunas_esperadas:
            assert col in df.columns, f"Coluna {col!r} ausente"

    def test_filtro_por_classificacao(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundos_resumo(filtro_classificacao="MONITORAR")
        assert len(df) == 1
        assert str(df.iloc[0]["CNPJ_FUNDO"]) == "12345678000195"

    def test_filtro_inexistente_retorna_vazio(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundos_resumo(filtro_classificacao="INEXISTENTE")
        assert df.empty


# ---------------------------------------------------------------------------
# Testes: get_safari_oportunidades
# ---------------------------------------------------------------------------

class TestGetSafariOportunidades:

    def test_retorna_apenas_acima_score_min(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_safari_oportunidades(score_min=30.0)
        assert isinstance(df, pd.DataFrame)
        assert all(df["SCORE_SAFARI"] >= 30.0)

    def test_retorna_dataframe_vazio_sem_csv(self, tmp_path: Path) -> None:
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path / "vazio"
            df = get_safari_oportunidades()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_ordenado_por_score_desc(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_safari_oportunidades(score_min=0.0)
        scores = df["SCORE_SAFARI"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_colunas_garantidas(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_safari_oportunidades(score_min=0.0)
        for col in ["CNPJ_FUNDO", "SCORE_SAFARI", "NPL_RATIO"]:
            assert col in df.columns


# ---------------------------------------------------------------------------
# Testes: get_distressed_npl
# ---------------------------------------------------------------------------

class TestGetDistressedNpl:

    def test_retorna_apenas_acima_npl_min(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_distressed_npl(npl_min=20.0)
        assert len(df) == 1
        assert df.iloc[0]["NPL_RATIO"] >= 20.0

    def test_retorna_dataframe_vazio_sem_csv(self, tmp_path: Path) -> None:
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path / "vazio"
            df = get_distressed_npl()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_ordenado_por_npl_desc(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_distressed_npl(npl_min=0.0)
        npls = df["NPL_RATIO"].tolist()
        assert npls == sorted(npls, reverse=True)


# ---------------------------------------------------------------------------
# Testes: get_fundo_historico
# ---------------------------------------------------------------------------

class TestGetFundoHistorico:

    def test_retorna_historico_do_cnpj(
        self, tmp_path: Path
    ) -> None:
        dados_hist = [
            {"CNPJ_FUNDO": "12345678000195", "DATA_COMPETENCIA": f"2025-{m:02d}",
             "NPL_RATIO": 25.0 + m, "ZUMBI_RATIO": 10.0, "LIQUIDEZ_IMEDIATA_RATIO": 0.3,
             "SCORE_SAFARI": 40.0, "ATIVO_TOTAL": 50_000_000.0,
             "PATRIMONIO_LIQUIDO": 35_000_000.0, "CLASSIFICACAO_SAFARI": "MONITORAR"}
            for m in range(1, 4)
        ]
        _create_monitor_csv(tmp_path, dados_hist)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundo_historico("12345678000195")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_cnpj_inexistente_retorna_vazio(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundo_historico("00000000000000")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_sem_csv_retorna_vazio(self, tmp_path: Path) -> None:
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path / "vazio"
            df = get_fundo_historico("12345678000195")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_cnpj_formatado_e_normalizado(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        """CNPJ com formatação deve funcionar."""
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            df = get_fundo_historico("12.345.678/0001-95")
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# Testes: get_estatisticas_gerais
# ---------------------------------------------------------------------------

class TestGetEstatisticasGerais:

    def test_retorna_dict_com_chaves_esperadas(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            stats = get_estatisticas_gerais()
        assert isinstance(stats, dict)
        assert "total_fundos" in stats
        assert "media_npl" in stats
        assert "media_score" in stats
        assert "total_ativo" in stats
        assert "error" not in stats

    def test_sem_csv_retorna_erro(self, tmp_path: Path) -> None:
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path / "vazio"
            stats = get_estatisticas_gerais()
        assert isinstance(stats, dict)
        assert "error" in stats

    def test_valores_numericos_corretos(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            stats = get_estatisticas_gerais()
        # 3 CNPJs únicos
        assert stats["total_fundos"] == 3
        # Média NPL = (5 + 30 + 0) / 3 = 11.67
        assert stats["media_npl"] == pytest.approx(11.67, abs=0.1)


# ---------------------------------------------------------------------------
# Testes: listar_exports_disponiveis
# ---------------------------------------------------------------------------

class TestListarExportsDisponiveis:

    def test_retorna_lista_com_exports(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        _create_monitor_csv(tmp_path, dados_monitor)
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            exports = listar_exports_disponiveis()
        assert isinstance(exports, list)
        assert len(exports) == 1
        assert "arquivo" in exports[0]
        assert "tamanho_mb" in exports[0]

    def test_retorna_lista_vazia_sem_csv(self, tmp_path: Path) -> None:
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path / "vazio"
            exports = listar_exports_disponiveis()
        assert exports == []

    def test_multiplos_exports_ordenados_por_recencia(
        self, tmp_path: Path, dados_monitor: List[Dict[str, Any]]
    ) -> None:
        """Múltiplos exports devem ser listados do mais recente para o mais antigo."""
        _create_monitor_csv(tmp_path, dados_monitor, "fidc_monitor_completo_20250101_120000.csv")
        _create_monitor_csv(tmp_path, dados_monitor, "fidc_monitor_completo_20250201_120000.csv")
        with patch("src.backend.settings") as mock_settings:
            mock_settings.output_dir = tmp_path
            exports = listar_exports_disponiveis()
        assert len(exports) == 2

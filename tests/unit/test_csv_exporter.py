"""
Testes para CSVExporter.
"""
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from src.persistence.csv_exporter import CSVExporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_exporter(tmp_path: Path) -> CSVExporter:
    """Cria CSVExporter apontando para diretório temporário."""
    return CSVExporter(output_dir=tmp_path)


def _make_dados(n: int = 3, score: float = 50.0, npl: float = 30.0) -> List[Dict[str, Any]]:
    """Gera lista de dicts com campos mínimos para exportação."""
    return [
        {
            "CNPJ_FUNDO": f"1234567800019{i}",
            "DATA_COMPETENCIA": f"2025-{i + 1:02d}",
            "ATIVO_TOTAL": 1_000_000.0 * (i + 1),
            "NPL_RATIO": npl,
            "SCORE_SAFARI": score,
            "CLASSIFICACAO_SAFARI": "MONITORAR",
            "STATUS": "SUCESSO",
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Testes: export_monitor_completo
# ---------------------------------------------------------------------------

class TestExportMonitorCompleto:

    def test_exporta_csv_com_dados_validos(
        self, tmp_output_dir: Path, lista_dados_fundos: List[Dict[str, Any]]
    ) -> None:
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_monitor_completo(lista_dados_fundos, timestamp="20250115_120000")
        assert path is not None
        assert path.exists()
        assert path.suffix == ".csv"

    def test_retorna_none_com_lista_vazia(self, tmp_output_dir: Path) -> None:
        exporter = _make_exporter(tmp_output_dir)
        result = exporter.export_monitor_completo([])
        assert result is None

    def test_filtra_apenas_status_sucesso(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(2)
        dados[0]["STATUS"] = "ERRO"
        dados[1]["STATUS"] = "SUCESSO"
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_monitor_completo(dados, timestamp="test")
        assert path is not None
        df = pd.read_csv(path, encoding="utf-8-sig")
        assert len(df) == 1
        assert str(df.iloc[0]["CNPJ_FUNDO"]) == str(dados[1]["CNPJ_FUNDO"])

    def test_retorna_none_quando_todos_com_erro(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(2)
        for d in dados:
            d["STATUS"] = "ERRO"
        exporter = _make_exporter(tmp_output_dir)
        result = exporter.export_monitor_completo(dados, timestamp="test")
        assert result is None

    def test_remove_duplicatas(self, tmp_output_dir: Path) -> None:
        """Registros com mesmo CNPJ+DATA_COMPETENCIA são deduplicados."""
        dados = _make_dados(1)
        dados_dup = dados + [{**dados[0]}]  # duplicata exata
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_monitor_completo(dados_dup, timestamp="dedup_test")
        assert path is not None
        df = pd.read_csv(path, encoding="utf-8-sig")
        assert len(df) == 1

    def test_cria_diretorio_pai_se_nao_existir(self, tmp_path: Path) -> None:
        subdir = tmp_path / "nested" / "dir"
        exporter = _make_exporter(subdir)
        dados = _make_dados(1)
        path = exporter.export_monitor_completo(dados, timestamp="test")
        assert path is not None
        assert path.exists()

    def test_filtra_cnpj_invalido(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(1)
        dados.append({**dados[0], "CNPJ_FUNDO": "0"})  # CNPJ inválido
        dados.append({**dados[0], "CNPJ_FUNDO": "nan"})  # CNPJ inválido
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_monitor_completo(dados, timestamp="cnpj_test")
        assert path is not None
        df = pd.read_csv(path, encoding="utf-8-sig")
        assert len(df) == 1


# ---------------------------------------------------------------------------
# Testes: export_safari_oportunidades
# ---------------------------------------------------------------------------

class TestExportSafariOportunidades:

    def test_exporta_apenas_acima_score_min(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(3, score=50.0)
        dados[0]["SCORE_SAFARI"] = 20.0  # abaixo de 30
        dados[1]["SCORE_SAFARI"] = 35.0  # acima
        dados[2]["SCORE_SAFARI"] = 60.0  # acima
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_safari_oportunidades(dados, score_min=30.0, timestamp="safari")
        assert path is not None
        df = pd.read_csv(path, encoding="utf-8-sig")
        assert len(df) == 2
        assert all(df["SCORE_SAFARI"] >= 30.0)

    def test_retorna_none_quando_nenhum_acima_do_score_min(
        self, tmp_output_dir: Path
    ) -> None:
        dados = _make_dados(2, score=10.0)
        exporter = _make_exporter(tmp_output_dir)
        result = exporter.export_safari_oportunidades(dados, score_min=30.0, timestamp="test")
        assert result is None

    def test_retorna_none_com_lista_vazia(self, tmp_output_dir: Path) -> None:
        exporter = _make_exporter(tmp_output_dir)
        result = exporter.export_safari_oportunidades([])
        assert result is None

    def test_ordenado_por_score_decrescente(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(3)
        dados[0]["SCORE_SAFARI"] = 35.0
        dados[1]["SCORE_SAFARI"] = 70.0
        dados[2]["SCORE_SAFARI"] = 50.0
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_safari_oportunidades(dados, score_min=30.0, timestamp="sort")
        assert path is not None
        df = pd.read_csv(path, encoding="utf-8-sig")
        scores = df["SCORE_SAFARI"].tolist()
        assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# Testes: export_distressed_npl
# ---------------------------------------------------------------------------

class TestExportDistressedNpl:

    def test_exporta_apenas_acima_npl_min(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(3)
        dados[0]["NPL_RATIO"] = 10.0  # abaixo de 20
        dados[1]["NPL_RATIO"] = 25.0  # acima
        dados[2]["NPL_RATIO"] = 50.0  # acima
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_distressed_npl(dados, npl_min=20.0, timestamp="npl")
        assert path is not None
        df = pd.read_csv(path, encoding="utf-8-sig")
        assert len(df) == 2
        assert all(df["NPL_RATIO"] >= 20.0)

    def test_retorna_none_quando_nenhum_acima_do_npl_min(
        self, tmp_output_dir: Path
    ) -> None:
        dados = _make_dados(2, npl=5.0)
        exporter = _make_exporter(tmp_output_dir)
        result = exporter.export_distressed_npl(dados, npl_min=20.0, timestamp="test")
        assert result is None

    def test_retorna_none_com_lista_vazia(self, tmp_output_dir: Path) -> None:
        exporter = _make_exporter(tmp_output_dir)
        result = exporter.export_distressed_npl([])
        assert result is None

    def test_nome_arquivo_inclui_npl_min(self, tmp_output_dir: Path) -> None:
        dados = _make_dados(1, npl=30.0)
        exporter = _make_exporter(tmp_output_dir)
        path = exporter.export_distressed_npl(dados, npl_min=25.0, timestamp="ts")
        assert path is not None
        assert "25" in path.name

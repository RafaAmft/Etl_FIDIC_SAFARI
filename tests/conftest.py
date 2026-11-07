"""
Fixtures compartilhadas para todos os testes do FIDC ETL.
"""
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Caminhos de fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_XML_PATH = FIXTURES_DIR / "sample_informe_mensal.xml"


# ---------------------------------------------------------------------------
# Fixtures de XML
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_xml_bytes() -> bytes:
    """XML real de FIDC (sample_informe_mensal.xml) em bytes."""
    return SAMPLE_XML_PATH.read_bytes()


@pytest.fixture
def sample_xml_bytes_encoded(sample_xml_bytes: bytes) -> bytes:
    """XML de FIDC codificado em Base64 (simula resposta da API B3)."""
    import base64
    return base64.b64encode(sample_xml_bytes)


@pytest.fixture
def malformed_xml_bytes() -> bytes:
    """XML malformado para testar tratamento de erros."""
    return b"<broken>xml without closing tag"


# ---------------------------------------------------------------------------
# Fixtures de dados de fundos
# ---------------------------------------------------------------------------

@pytest.fixture
def dados_fundo_valido() -> Dict[str, Any]:
    """Dict com campos mínimos para um fundo válido (sem anomalias)."""
    return {
        "CNPJ_FUNDO": "31405473000100",
        "DATA_COMPETENCIA": "2025-11",
        "ATIVO_TOTAL": 91_630_153.13,
        "CARTEIRA_TOTAL": 71_326_638.62,
        "DISPONIBILIDADES": 691.88,
        "PASSIVO_CIRCULANTE": 86_999.28,
        "PATRIMONIO_LIQUIDO": 70_120_726.17,
        "CREDITOS_ADQUIRIDOS": 69_823_801.79,
        "CRED_VENCIDOS_INADIMPLENTES": 0.0,
        "CRED_TOTAL_VENC_INADIMPL": 0.0,
        "CRED_INADIMPLENCIA": 69_823_801.79,
        "DICRED_INADIMPLENCIA": 0.0,
        "AGING_VENC_MAIOR_1080_DIAS": 0.0,
    }


@pytest.fixture
def dados_fundo_distressed() -> Dict[str, Any]:
    """Dict com campos de fundo distressed (NPL alto, liquidez baixa)."""
    return {
        "CNPJ_FUNDO": "12345678000195",
        "DATA_COMPETENCIA": "2025-10",
        "ATIVO_TOTAL": 50_000_000.0,
        "CARTEIRA_TOTAL": 40_000_000.0,
        "DISPONIBILIDADES": 500_000.0,
        "PASSIVO_CIRCULANTE": 2_000_000.0,
        "PATRIMONIO_LIQUIDO": 35_000_000.0,
        "CREDITOS_ADQUIRIDOS": 38_000_000.0,
        "CRED_VENCIDOS_INADIMPLENTES": 12_000_000.0,
        "CRED_TOTAL_VENC_INADIMPL": 12_000_000.0,
        "CRED_INADIMPLENCIA": 12_000_000.0,
        "DICRED_INADIMPLENCIA": 0.0,
        "AGING_VENC_MAIOR_1080_DIAS": 5_000_000.0,
        # Métricas calculadas (para testes de anomalia/scoring)
        "NPL_RATIO": 30.0,        # 12M / 40M = 30%
        "ZUMBI_RATIO": 12.5,      # 5M / 40M = 12.5%
        "LIQUIDEZ_IMEDIATA_RATIO": 0.25,  # 500k / 2M
        "ANOMALIA_ESTRUTURAL": False,
    }


@pytest.fixture
def dados_fundo_anomalia() -> Dict[str, Any]:
    """Dict com dados anômalos (NPL implausível)."""
    return {
        "CNPJ_FUNDO": "99999999000191",
        "DATA_COMPETENCIA": "2025-09",
        "ATIVO_TOTAL": 1_000.0,
        "CARTEIRA_TOTAL": 500.0,   # < R$ 1.000 → FLAG_CARTEIRA_ZERADA
        "DISPONIBILIDADES": 0.0,
        "PASSIVO_CIRCULANTE": 0.0,
        "PATRIMONIO_LIQUIDO": 0.0,
        "CREDITOS_ADQUIRIDOS": 0.0,
        "CRED_TOTAL_VENC_INADIMPL": 0.0,
        "NPL_RATIO": 0.0,
        "ZUMBI_RATIO": 0.0,
        "LIQUIDEZ_IMEDIATA_RATIO": 0.0,
        "ANOMALIA_ESTRUTURAL": True,
    }


@pytest.fixture
def lista_dados_fundos(
    dados_fundo_valido: Dict[str, Any],
    dados_fundo_distressed: Dict[str, Any],
    dados_fundo_anomalia: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Lista com registros de múltiplos fundos para testes de export."""
    valido_sucesso = {**dados_fundo_valido, "STATUS": "SUCESSO", "SCORE_SAFARI": 10.0,
                     "CLASSIFICACAO_SAFARI": "SEM_OPORTUNIDADE", "ANOMALIA_ESTRUTURAL": False}
    distressed_sucesso = {**dados_fundo_distressed, "STATUS": "SUCESSO", "SCORE_SAFARI": 45.5,
                          "CLASSIFICACAO_SAFARI": "MONITORAR"}
    anomalia_sucesso = {**dados_fundo_anomalia, "STATUS": "SUCESSO", "SCORE_SAFARI": 0.0,
                        "CLASSIFICACAO_SAFARI": "DADOS_INVALIDOS"}
    return [valido_sucesso, distressed_sucesso, anomalia_sucesso]


# ---------------------------------------------------------------------------
# Fixtures de diretórios temporários
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Diretório temporário para exports CSV."""
    output = tmp_path / "output"
    output.mkdir()
    return output


@pytest.fixture
def tmp_cache_dir(tmp_path: Path) -> Path:
    """Diretório temporário para cache."""
    cache = tmp_path / "cache"
    cache.mkdir()
    return cache


@pytest.fixture
def sample_csv_path(tmp_path: Path, lista_dados_fundos: List[Dict[str, Any]]) -> Path:
    """CSV de monitor completo com dados de teste."""
    import pandas as pd
    output_dir = tmp_path / "output" / "2025-01-15"
    output_dir.mkdir(parents=True)
    csv_path = output_dir / "fidc_monitor_completo_20250115_120000.csv"
    df = pd.DataFrame(lista_dados_fundos)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path

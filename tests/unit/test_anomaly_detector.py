"""
Testes para AnomalyDetector.
"""
from typing import Any, Dict

import pytest

from src.transformation.anomaly_detector import AnomalyDetector
from src.utils.constants import (
    FLAG_CRED_ADQ_BAIXO,
    FLAG_CARTEIRA_ZERADA,
    FLAG_NPL_IMPLAUSIVEL,
    FLAG_VENCIDOS_ALTO,
    STATUS_ANOMALIA_CONTABIL,
    STATUS_ANOMALIA_ESTRUTURAL,
    STATUS_CARTEIRA_ZERADA,
    STATUS_NPL_IMPLAUSIVEL,
    STATUS_VALIDADO,
)


# ---------------------------------------------------------------------------
# Dados base válidos (sem anomalias)
# ---------------------------------------------------------------------------

DADOS_VALIDOS: Dict[str, Any] = {
    "CARTEIRA_TOTAL": 10_000_000.0,
    "CREDITOS_ADQUIRIDOS": 9_500_000.0,   # 95% da carteira → OK
    "CRED_TOTAL_VENC_INADIMPL": 1_000_000.0,  # 10% da carteira → OK
    "NPL_RATIO": 10.0,                     # 10% → não implausível
}


class TestFlag1CredAdqBaixo:
    """Flag FLAG_CRED_ADQ_BAIXO: CREDITOS_ADQUIRIDOS < 1% da carteira."""

    def test_flag_inativo_quando_cred_normal(self) -> None:
        result = AnomalyDetector.detect_anomalies({**DADOS_VALIDOS})
        assert result[FLAG_CRED_ADQ_BAIXO] is False

    def test_flag_ativo_quando_cred_baixo(self) -> None:
        dados = {**DADOS_VALIDOS, "CREDITOS_ADQUIRIDOS": 50_000.0}  # 0.5% < 1%
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_CRED_ADQ_BAIXO] is True

    def test_flag_inativo_quando_carteira_zero(self) -> None:
        """Carteira zero → não divide → flag deve ser False."""
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 0.0, "CREDITOS_ADQUIRIDOS": 0.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_CRED_ADQ_BAIXO] is False

    def test_flag_exatamente_no_threshold(self) -> None:
        """CREDITOS_ADQUIRIDOS = 1% da carteira → não ativa flag (< não <=)."""
        dados = {**DADOS_VALIDOS, "CREDITOS_ADQUIRIDOS": 100_000.0}  # 1% exato
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_CRED_ADQ_BAIXO] is False


class TestFlag2VencidosAlto:
    """Flag FLAG_VENCIDOS_ALTO: vencidos > 150% da carteira."""

    def test_flag_inativo_quando_vencidos_normal(self) -> None:
        result = AnomalyDetector.detect_anomalies({**DADOS_VALIDOS})
        assert result[FLAG_VENCIDOS_ALTO] is False

    def test_flag_ativo_quando_vencidos_excessivos(self) -> None:
        dados = {
            **DADOS_VALIDOS,
            "CRED_TOTAL_VENC_INADIMPL": 20_000_000.0,  # 200% > 150%
        }
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_VENCIDOS_ALTO] is True

    def test_flag_inativo_quando_carteira_zero(self) -> None:
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 0.0, "CRED_TOTAL_VENC_INADIMPL": 0.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_VENCIDOS_ALTO] is False

    def test_flag_exatamente_150_percent_nao_ativa(self) -> None:
        """Exatamente 150% → não ativa (> não >=)."""
        dados = {**DADOS_VALIDOS, "CRED_TOTAL_VENC_INADIMPL": 15_000_000.0}  # 150%
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_VENCIDOS_ALTO] is False


class TestFlag3NplImplausivel:
    """Flag FLAG_NPL_IMPLAUSIVEL: NPL > 500%."""

    def test_flag_inativo_npl_normal(self) -> None:
        result = AnomalyDetector.detect_anomalies({**DADOS_VALIDOS})
        assert result[FLAG_NPL_IMPLAUSIVEL] is False

    def test_flag_inativo_npl_distressed(self) -> None:
        dados = {**DADOS_VALIDOS, "NPL_RATIO": 80.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_NPL_IMPLAUSIVEL] is False

    def test_flag_ativo_npl_acima_500(self) -> None:
        dados = {**DADOS_VALIDOS, "NPL_RATIO": 600.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_NPL_IMPLAUSIVEL] is True

    def test_flag_inativo_npl_exatamente_500(self) -> None:
        """500% exato não ativa (> não >=)."""
        dados = {**DADOS_VALIDOS, "NPL_RATIO": 500.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_NPL_IMPLAUSIVEL] is False


class TestFlag4CarteiraZerada:
    """Flag FLAG_CARTEIRA_ZERADA: carteira < R$ 1.000."""

    def test_flag_inativo_carteira_normal(self) -> None:
        result = AnomalyDetector.detect_anomalies({**DADOS_VALIDOS})
        assert result[FLAG_CARTEIRA_ZERADA] is False

    def test_flag_ativo_carteira_zerada(self) -> None:
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 0.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_CARTEIRA_ZERADA] is True

    def test_flag_ativo_carteira_muito_pequena(self) -> None:
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 500.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_CARTEIRA_ZERADA] is True

    def test_flag_inativo_carteira_igual_threshold(self) -> None:
        """R$ 1.000 exato → não ativa (< não <=)."""
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 1_000.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result[FLAG_CARTEIRA_ZERADA] is False


class TestStatusDados:
    """Testes para a classificação STATUS_DADOS."""

    def test_status_validado_sem_anomalias(self) -> None:
        result = AnomalyDetector.detect_anomalies({**DADOS_VALIDOS})
        assert result["STATUS_DADOS"] == STATUS_VALIDADO
        assert result["ANOMALIA_ESTRUTURAL"] is False

    def test_status_carteira_zerada_tem_prioridade(self) -> None:
        """Carteira zerada tem prioridade máxima sobre NPL implausível."""
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 0.0, "NPL_RATIO": 999.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result["STATUS_DADOS"] == STATUS_CARTEIRA_ZERADA

    def test_status_npl_implausivel(self) -> None:
        dados = {**DADOS_VALIDOS, "NPL_RATIO": 600.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result["STATUS_DADOS"] == STATUS_NPL_IMPLAUSIVEL

    def test_status_anomalia_contabil_vencidos_altos(self) -> None:
        dados = {**DADOS_VALIDOS, "CRED_TOTAL_VENC_INADIMPL": 20_000_000.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result["STATUS_DADOS"] == STATUS_ANOMALIA_CONTABIL

    def test_status_anomalia_estrutural_cred_baixo(self) -> None:
        """FLAG_CRED_ADQ_BAIXO sem outros flags → STATUS_ANOMALIA_ESTRUTURAL."""
        dados = {**DADOS_VALIDOS, "CREDITOS_ADQUIRIDOS": 50_000.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result["STATUS_DADOS"] == STATUS_ANOMALIA_ESTRUTURAL

    def test_anomalia_estrutural_consolidada(self) -> None:
        """ANOMALIA_ESTRUTURAL = True quando qualquer flag está ativo."""
        dados = {**DADOS_VALIDOS, "CARTEIRA_TOTAL": 100.0}
        result = AnomalyDetector.detect_anomalies(dados)
        assert result["ANOMALIA_ESTRUTURAL"] is True


class TestAddAnomalyFlags:
    """Testes para add_anomaly_flags (in-place)."""

    def test_adiciona_flags_ao_dict(self) -> None:
        dados: Dict[str, Any] = {**DADOS_VALIDOS}
        AnomalyDetector.add_anomaly_flags(dados)
        assert FLAG_CRED_ADQ_BAIXO in dados
        assert FLAG_VENCIDOS_ALTO in dados
        assert FLAG_NPL_IMPLAUSIVEL in dados
        assert FLAG_CARTEIRA_ZERADA in dados
        assert "ANOMALIA_ESTRUTURAL" in dados
        assert "STATUS_DADOS" in dados

    def test_dados_vazios_nao_levanta_excecao(self) -> None:
        dados: Dict[str, Any] = {}
        AnomalyDetector.add_anomaly_flags(dados)
        assert "STATUS_DADOS" in dados

"""
Testes para MetricsCalculator.
"""
from typing import Any, Dict

import pytest

from src.transformation.metrics_calculator import FundMetrics, MetricsCalculator


class TestCalculateNpl:
    """Testes para MetricsCalculator.calculate_npl."""

    def test_npl_normal(self) -> None:
        npl = MetricsCalculator.calculate_npl(200_000, 1_000_000)
        assert npl == pytest.approx(20.0)

    def test_npl_zero_creditos(self) -> None:
        npl = MetricsCalculator.calculate_npl(0, 1_000_000)
        assert npl == pytest.approx(0.0)

    def test_npl_zero_base_retorna_zero(self) -> None:
        """Divisão por zero deve retornar 0.0, não lançar exceção."""
        npl = MetricsCalculator.calculate_npl(200_000, 0)
        assert npl == pytest.approx(0.0)

    def test_npl_acima_100_percent(self) -> None:
        """NPL pode ser > 100% em casos distressed."""
        npl = MetricsCalculator.calculate_npl(150_000, 100_000)
        assert npl == pytest.approx(150.0)

    def test_npl_negativo_base_retorna_zero(self) -> None:
        npl = MetricsCalculator.calculate_npl(100, -1)
        assert npl == pytest.approx(0.0)


class TestCalculateLiquidez:
    """Testes para MetricsCalculator.calculate_liquidez."""

    def test_liquidez_normal(self) -> None:
        liq = MetricsCalculator.calculate_liquidez(500_000, 1_000_000)
        assert liq == pytest.approx(0.5)

    def test_liquidez_acima_um(self) -> None:
        liq = MetricsCalculator.calculate_liquidez(2_000_000, 1_000_000)
        assert liq == pytest.approx(2.0)

    def test_liquidez_passivo_zero_retorna_zero(self) -> None:
        liq = MetricsCalculator.calculate_liquidez(500_000, 0)
        assert liq == pytest.approx(0.0)


class TestCalculateZumbiRatio:
    """Testes para MetricsCalculator.calculate_zumbi_ratio."""

    def test_zumbi_normal(self) -> None:
        zumbi = MetricsCalculator.calculate_zumbi_ratio(10_000_000, 40_000_000)
        assert zumbi == pytest.approx(25.0)

    def test_zumbi_zero_aging(self) -> None:
        zumbi = MetricsCalculator.calculate_zumbi_ratio(0, 40_000_000)
        assert zumbi == pytest.approx(0.0)

    def test_zumbi_carteira_zero_retorna_zero(self) -> None:
        zumbi = MetricsCalculator.calculate_zumbi_ratio(1_000_000, 0)
        assert zumbi == pytest.approx(0.0)


class TestCalculateAllMetrics:
    """Testes para MetricsCalculator.calculate_all_metrics."""

    def test_metricas_completas(self, dados_fundo_distressed: Dict[str, Any]) -> None:
        metrics = MetricsCalculator.calculate_all_metrics(dados_fundo_distressed)
        assert isinstance(metrics, FundMetrics)
        assert metrics.npl_ratio == pytest.approx(30.0)
        assert metrics.zumbi_ratio == pytest.approx(12.5)
        assert metrics.liquidez_imediata_ratio == pytest.approx(0.25)

    def test_metricas_com_dados_vazios(self) -> None:
        dados_vazios: Dict[str, Any] = {}
        metrics = MetricsCalculator.calculate_all_metrics(dados_vazios)
        assert metrics.npl_ratio == pytest.approx(0.0)
        assert metrics.zumbi_ratio == pytest.approx(0.0)
        assert metrics.liquidez_imediata_ratio == pytest.approx(0.0)

    def test_add_metrics_to_data(self, dados_fundo_distressed: Dict[str, Any]) -> None:
        """Verifica que add_metrics_to_data modifica o dict in-place."""
        dados = {**dados_fundo_distressed}
        MetricsCalculator.add_metrics_to_data(dados)

        assert "NPL_RATIO" in dados
        assert "ZUMBI_RATIO" in dados
        assert "LIQUIDEZ_IMEDIATA_RATIO" in dados
        assert "NPL_OUTLIER_FLAG" in dados
        assert dados["NPL_OUTLIER_FLAG"] is False  # NPL 30% não é outlier

    def test_npl_outlier_flag_ativo(self) -> None:
        """NPL > 1000% deve ativar flag de outlier."""
        dados: Dict[str, Any] = {
            "CARTEIRA_TOTAL": 100_000.0,
            "CRED_TOTAL_VENC_INADIMPL": 2_000_000.0,  # 2000% NPL
        }
        MetricsCalculator.add_metrics_to_data(dados)
        assert dados["NPL_OUTLIER_FLAG"] is True

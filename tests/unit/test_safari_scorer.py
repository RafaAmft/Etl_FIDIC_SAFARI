"""
Testes para SafariScorer.
"""
from typing import Any, Dict

import pytest

from src.transformation.safari_scorer import SafariScorer
from src.utils.constants import (
    LABEL_ALTA_OPORTUNIDADE,
    LABEL_BAIXA_PRIORIDADE,
    LABEL_DADOS_INVALIDOS,
    LABEL_MONITORAR,
    LABEL_OPORTUNIDADE_MODERADA,
    LABEL_SEM_OPORTUNIDADE,
    SCORE_ALTA_OPORTUNIDADE,
    SCORE_BAIXA_PRIORIDADE,
    SCORE_MONITORAR,
    SCORE_OPORTUNIDADE_MODERADA,
)


# ---------------------------------------------------------------------------
# Dados base para compor cenários
# ---------------------------------------------------------------------------

DADOS_ANOMALIA: Dict[str, Any] = {"ANOMALIA_ESTRUTURAL": True}

DADOS_DISTRESSED_ALTO: Dict[str, Any] = {
    "ANOMALIA_ESTRUTURAL": False,
    "NPL_RATIO": 100.0,           # dentro da faixa 15-150% → 40 * (100/150) ≈ 26.67 pts NPL
    "ZUMBI_RATIO": 50.0,          # 50/50 * 25 = 25 pts Zumbi
    "LIQUIDEZ_IMEDIATA_RATIO": 0.0,  # (1-0) * 25 = 25 pts Liquidez
    "ATIVO_TOTAL": 200_000_000.0, # ≥ R$ 100M → 10 pts Porte
}


class TestScoreZeroParaAnomalias:
    """Fundos com anomalia estrutural devem ter score 0 e label DADOS_INVALIDOS."""

    def test_anomalia_estrutural_retorna_score_zero(self) -> None:
        result = SafariScorer.calculate_safari_score(DADOS_ANOMALIA)
        assert result["SCORE_SAFARI"] == 0.0

    def test_anomalia_estrutural_retorna_label_invalido(self) -> None:
        result = SafariScorer.calculate_safari_score(DADOS_ANOMALIA)
        assert result["CLASSIFICACAO_SAFARI"] == LABEL_DADOS_INVALIDOS

    def test_dados_vazios_nao_sao_anomalia(self) -> None:
        """Dados vazios sem flag ANOMALIA_ESTRUTURAL devem gerar score (baixo, mas > 0 possível)."""
        result = SafariScorer.calculate_safari_score({})
        assert "SCORE_SAFARI" in result
        assert "CLASSIFICACAO_SAFARI" in result
        assert result["CLASSIFICACAO_SAFARI"] != LABEL_DADOS_INVALIDOS


class TestComponenteNPL:
    """Testes para o componente NPL (peso 40)."""

    def test_npl_abaixo_minimo_nao_pontua(self) -> None:
        """NPL < 15% → não é considerado distressed → 0 pontos NPL."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 5.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(0.0)

    def test_npl_dentro_da_faixa_pontua(self) -> None:
        """NPL = 150% (máximo) → pontuação máxima de NPL = 40."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 150.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(40.0)

    def test_npl_acima_maximo_pontua_apenas_5(self) -> None:
        """NPL > 150% → penalidade de apenas 5 pontos."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 200.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(5.0)


class TestComponenteZumbi:
    """Testes para o componente Zumbi Ratio (peso 25)."""

    def test_zumbi_zero_nao_pontua(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(0.0)

    def test_zumbi_maximo_pontua_25(self) -> None:
        """Zumbi >= 50% → pontuação máxima 25."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 50.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(25.0)

    def test_zumbi_intermediario(self) -> None:
        """Zumbi = 25% → 25/50 * 25 = 12.5 pontos."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 25.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(12.5)


class TestComponenteLiquidez:
    """Testes para o componente Liquidez (peso 25)."""

    def test_liquidez_zero_pontua_25(self) -> None:
        """Liquidez = 0 → máxima pressão → 25 pontos."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 0.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(25.0)

    def test_liquidez_acima_de_1_nao_pontua(self) -> None:
        """Liquidez >= 1.0 → sem pressão → 0 pontos."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.5,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(0.0)

    def test_liquidez_meio(self) -> None:
        """Liquidez = 0.5 → (1 - 0.5) * 25 = 12.5 pontos."""
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 0.5,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(12.5)


class TestComponentePorte:
    """Testes para o componente Porte (10 pontos)."""

    def test_grande_porte_pontua_10(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 100_000_000.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(10.0)

    def test_medio_porte_pontua_7(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 50_000_000.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(7.0)

    def test_pequeno_porte_pontua_5(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 10_000_000.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(5.0)

    def test_micro_porte_pontua_2(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 1_000_000.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(2.0)

    def test_abaixo_micro_nao_pontua(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 500_000.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["SCORE_SAFARI"] == pytest.approx(0.0)


class TestClassificacaoScore:
    """Testes para os tiers de classificação."""

    def _score_para(self, score_target: float) -> str:
        """Retorna classificação para um dado score simulado via ATIVO."""
        # Usamos apenas o componente de porte para simular scores simples
        # Para scores compostos precisamos montar dados específicos
        dados = {"ANOMALIA_ESTRUTURAL": False, "NPL_RATIO": 0.0,
                 "ZUMBI_RATIO": 0.0, "LIQUIDEZ_IMEDIATA_RATIO": 1.0, "ATIVO_TOTAL": 0.0}
        result = SafariScorer.calculate_safari_score(dados)
        return result["CLASSIFICACAO_SAFARI"]

    def test_score_zero_sem_oportunidade(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 0.0,
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 1.0,
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert result["CLASSIFICACAO_SAFARI"] == LABEL_SEM_OPORTUNIDADE

    def test_score_acima_70_alta_oportunidade(self) -> None:
        """Score máximo = 40 + 25 + 25 + 10 = 100 → ALTA_OPORTUNIDADE."""
        result = SafariScorer.calculate_safari_score(DADOS_DISTRESSED_ALTO)
        assert result["SCORE_SAFARI"] >= SCORE_ALTA_OPORTUNIDADE
        assert result["CLASSIFICACAO_SAFARI"] == LABEL_ALTA_OPORTUNIDADE

    def test_score_acima_50_oportunidade_moderada(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 100.0,    # ~26.67 pts
            "ZUMBI_RATIO": 30.0,   # 15 pts
            "LIQUIDEZ_IMEDIATA_RATIO": 0.7,  # 7.5 pts
            "ATIVO_TOTAL": 5_000_000.0,  # 0 pts (< 10M)
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert SCORE_OPORTUNIDADE_MODERADA <= result["SCORE_SAFARI"] < SCORE_ALTA_OPORTUNIDADE
        assert result["CLASSIFICACAO_SAFARI"] == LABEL_OPORTUNIDADE_MODERADA

    def test_score_acima_30_monitorar(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 60.0,   # 60/150 * 40 = 16 pts
            "ZUMBI_RATIO": 15.0,  # 15/50 * 25 = 7.5 pts
            "LIQUIDEZ_IMEDIATA_RATIO": 0.7,  # 7.5 pts
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert SCORE_MONITORAR <= result["SCORE_SAFARI"] < SCORE_OPORTUNIDADE_MODERADA
        assert result["CLASSIFICACAO_SAFARI"] == LABEL_MONITORAR

    def test_score_acima_15_baixa_prioridade(self) -> None:
        dados: Dict[str, Any] = {
            "ANOMALIA_ESTRUTURAL": False,
            "NPL_RATIO": 30.0,   # 30/150 * 40 = 8 pts
            "ZUMBI_RATIO": 0.0,
            "LIQUIDEZ_IMEDIATA_RATIO": 0.7,  # 7.5 pts
            "ATIVO_TOTAL": 0.0,
        }
        result = SafariScorer.calculate_safari_score(dados)
        assert SCORE_BAIXA_PRIORIDADE <= result["SCORE_SAFARI"] < SCORE_MONITORAR
        assert result["CLASSIFICACAO_SAFARI"] == LABEL_BAIXA_PRIORIDADE


class TestAddSafariScore:
    """Testes para add_safari_score (in-place)."""

    def test_adiciona_campos_ao_dict(self, dados_fundo_distressed: Dict[str, Any]) -> None:
        dados = {**dados_fundo_distressed}
        SafariScorer.add_safari_score(dados)
        assert "SCORE_SAFARI" in dados
        assert "CLASSIFICACAO_SAFARI" in dados

    def test_nao_sobrescreve_campos_existentes_sem_calcular(
        self, dados_fundo_distressed: Dict[str, Any]
    ) -> None:
        """add_safari_score SEMPRE recalcula e sobrescreve."""
        dados = {**dados_fundo_distressed}
        SafariScorer.add_safari_score(dados)
        # Apenas verifica que campos são do tipo correto após execução
        assert isinstance(dados["SCORE_SAFARI"], float)
        assert isinstance(dados["CLASSIFICACAO_SAFARI"], str)

"""
FIDC Safari Scorer — Sistema de scoring para identificação de oportunidades distressed.
"""
import logging
from typing import Any, Dict

from src.utils.constants import (
    ATIVO_GRANDE_PORTE,
    ATIVO_MEDIO_PORTE,
    ATIVO_MICRO_PORTE,
    ATIVO_PEQUENO_PORTE,
    LABEL_ALTA_OPORTUNIDADE,
    LABEL_BAIXA_PRIORIDADE,
    LABEL_DADOS_INVALIDOS,
    LABEL_MONITORAR,
    LABEL_OPORTUNIDADE_MODERADA,
    LABEL_SEM_OPORTUNIDADE,
    NPL_DISTRESSED_MAX,
    NPL_DISTRESSED_MIN,
    PESO_LIQUIDEZ,
    PESO_NPL,
    PESO_ZUMBI,
    PONTOS_GRANDE_PORTE,
    PONTOS_MEDIO_PORTE,
    PONTOS_MICRO_PORTE,
    PONTOS_PEQUENO_PORTE,
    SCORE_ALTA_OPORTUNIDADE,
    SCORE_BAIXA_PRIORIDADE,
    SCORE_MONITORAR,
    SCORE_OPORTUNIDADE_MODERADA,
    ZUMBI_SCORE_DIVISOR,
)

logger = logging.getLogger(__name__)


class SafariScorer:
    """Calculador do FIDC Safari Score."""

    @staticmethod
    def calculate_safari_score(dados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula FIDC Safari Score (0-100).

        Critérios:
        1. NPL (40%) — alto NPL dentro da faixa distressed (15-150%)
        2. Zumbi Ratio (25%) — carteira antiga/deteriorada
        3. Liquidez (25%) — quanto menor, mais pressão de caixa
        4. Porte (10%) — ativo mínimo para viabilidade

        Args:
            dados: Dicionário com dados do fundo.

        Returns:
            Dict com SCORE_SAFARI e CLASSIFICACAO_SAFARI.
        """
        if dados.get("ANOMALIA_ESTRUTURAL", False):
            return {"SCORE_SAFARI": 0.0, "CLASSIFICACAO_SAFARI": LABEL_DADOS_INVALIDOS}

        npl = float(dados.get("NPL_RATIO", 0) or 0)
        zumbi = float(dados.get("ZUMBI_RATIO", 0) or 0)
        liquidez = float(dados.get("LIQUIDEZ_IMEDIATA_RATIO", 0) or 0)
        ativo = float(dados.get("ATIVO_TOTAL", 0) or 0)

        score = 0.0

        # Componente 1: NPL (peso configurável)
        if NPL_DISTRESSED_MIN <= npl <= NPL_DISTRESSED_MAX:
            score += min((npl / NPL_DISTRESSED_MAX) * PESO_NPL, PESO_NPL)
        elif npl > NPL_DISTRESSED_MAX:
            score += 5.0  # Penalidade para NPL excessivo

        # Componente 2: Zumbi Ratio (peso configurável)
        if zumbi > 0:
            score += min((zumbi / ZUMBI_SCORE_DIVISOR) * PESO_ZUMBI, PESO_ZUMBI)

        # Componente 3: Liquidez — quanto menor, maior a pressão (maior score)
        if liquidez < 1.0:
            score += (1.0 - liquidez) * PESO_LIQUIDEZ

        # Componente 4: Porte
        if ativo >= ATIVO_GRANDE_PORTE:
            score += PONTOS_GRANDE_PORTE
        elif ativo >= ATIVO_MEDIO_PORTE:
            score += PONTOS_MEDIO_PORTE
        elif ativo >= ATIVO_PEQUENO_PORTE:
            score += PONTOS_PEQUENO_PORTE
        elif ativo >= ATIVO_MICRO_PORTE:
            score += PONTOS_MICRO_PORTE

        score = round(score, 2)

        # Classificação
        if score >= SCORE_ALTA_OPORTUNIDADE:
            classificacao = LABEL_ALTA_OPORTUNIDADE
        elif score >= SCORE_OPORTUNIDADE_MODERADA:
            classificacao = LABEL_OPORTUNIDADE_MODERADA
        elif score >= SCORE_MONITORAR:
            classificacao = LABEL_MONITORAR
        elif score >= SCORE_BAIXA_PRIORIDADE:
            classificacao = LABEL_BAIXA_PRIORIDADE
        else:
            classificacao = LABEL_SEM_OPORTUNIDADE

        return {"SCORE_SAFARI": score, "CLASSIFICACAO_SAFARI": classificacao}

    @staticmethod
    def add_safari_score(dados: Dict[str, Any]) -> None:
        """
        Calcula e adiciona Safari score ao dicionário (in-place).

        Args:
            dados: Dicionário a ser modificado in-place.
        """
        safari_data = SafariScorer.calculate_safari_score(dados)
        dados.update(safari_data)

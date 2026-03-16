"""
FIDC Safari Scorer - Sistema de scoring para identificação de oportunidades distressed.
"""
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class SafariScorer:
    """Calculador do FIDC Safari Score"""
    
    @staticmethod
    def calculate_safari_score(dados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcula FIDC Safari Score (0-100).
        
        Critérios para oportunidade:
        1. NPL alto (15-150%) → Distressed mas ainda operacional
        2. Zumbi Ratio alto → Carteira antiga/deteriorada
        3. Liquidez baixa → Pressão de caixa
        4. Ativo significativo → Porte mínimo para negociação
        
        Args:
            dados: Dicionário com dados do fundo
            
        Returns:
            Dict com score e classificação
        """
        anomalia_estrutural = dados.get('ANOMALIA_ESTRUTURAL', False)
        
        if anomalia_estrutural:
            # Se tem anomalia, score = 0 (excluir da análise)
            return {
                'SCORE_SAFARI': 0.0,
                'CLASSIFICACAO_SAFARI': 'DADOS_INVALIDOS'
            }
        
        npl = float(dados.get('NPL_RATIO', 0) or 0)
        zumbi = float(dados.get('ZUMBI_RATIO', 0) or 0)
        liquidez = float(dados.get('LIQUIDEZ_IMEDIATA_RATIO', 0) or 0)
        ativo = float(dados.get('ATIVO_TOTAL', 0) or 0)
        
        # Cálculo do score (0-100)
        score = 0.0
        
        # Componente 1: NPL (peso 40%)
        if 15 <= npl <= 150:
            score += min((npl / 150) * 40, 40)  # Max 40 pontos
        elif npl > 150:
            score += 5  # Penaliza NPL muito alto
        
        # Componente 2: Zumbi Ratio (peso 25%)
        if zumbi > 0:
            score += min((zumbi / 50) * 25, 25)  # Max 25 pontos
        
        # Componente 3: Liquidez (peso 25%) - Quanto menor, melhor
        if liquidez < 1.0:
            score += (1 - liquidez) * 25  # Max 25 pontos
        
        # Componente 4: Porte (peso 10%)
        if ativo >= 100_000_000:  # >= R$ 100 Mi
            score += 10
        elif ativo >= 50_000_000:  # >= R$ 50 Mi
            score += 7
        elif ativo >= 10_000_000:  # >= R$ 10 Mi
            score += 5
        elif ativo >= 1_000_000:  # >= R$ 1 Mi
            score += 2
        
        score = round(score, 2)
        
        # Classificação
        if score >= 70:
            classificacao = 'ALTA_OPORTUNIDADE'
        elif score >= 50:
            classificacao = 'OPORTUNIDADE_MODERADA'
        elif score >= 30:
            classificacao = 'MONITORAR'
        elif score >= 15:
            classificacao = 'BAIXA_PRIORIDADE'
        else:
            classificacao = 'SEM_OPORTUNIDADE'
        
        return {
            'SCORE_SAFARI': score,
            'CLASSIFICACAO_SAFARI': classificacao
        }
    
    @staticmethod
    def add_safari_score(dados: Dict[str, Any]) -> None:
        """
        Calcula e adiciona Safari score ao dicionário.
        
        Args:
            dados: Dicionário a ser modificado in-place
        """
        safari_data = SafariScorer.calculate_safari_score(dados)
        dados.update(safari_data)

"""
Calculador de métricas financeiras para análise de fundos distressed.
"""
from dataclasses import dataclass
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


@dataclass
class FundMetrics:
    """Métricas calculadas de um fundo"""
    npl_ratio: float
    npl_ratio_credito_puro: float
    liquidez_imediata_ratio: float
    zumbi_ratio: float
    indice_npl_percentual: float
    taxa_liquidez_percentual: float
    concentracao_credito_percentual: float
    inadimplencia_total: float


class MetricsCalculator:
    """Calculador de métricas financeiras"""
    
    @staticmethod
    def calculate_all_metrics(dados: Dict[str, Any]) -> FundMetrics:
        """
        Calcula todas as métricas financeiras.
        
        Args:
            dados: Dicionário com dados extraídos do XML
            
        Returns:
            FundMetrics: Objeto com todas as métricas calculadas
        """
        # Extrair valores necessários
        creditos_adquiridos = float(dados.get('CREDITOS_ADQUIRIDOS', 0) or 0)
        carteira_total = float(dados.get('CARTEIRA_TOTAL', 0) or 0)
        ativo_total = float(dados.get('ATIVO_TOTAL', 0) or 0)
        disponibilidades = float(dados.get('DISPONIBILIDADES', 0) or 0)
        passivo_circulante = float(dados.get('PASSIVO_CIRCULANTE', 0) or 0)
        aging_maior_1080 = float(dados.get('AGING_VENC_MAIOR_1080_DIAS', 0) or 0)
        inadimpl_cred = float(dados.get('CRED_INADIMPLENCIA', 0) or 0)
        inadimpl_dicred = float(dados.get('DICRED_INADIMPLENCIA', 0) or 0)
        creditos_vencidos_total = float(dados.get('CRED_TOTAL_VENC_INADIMPL', 0) or 0)
        
        # Calcular inadimplência total
        inadimplencia_total = max(inadimpl_cred, inadimpl_dicred)
        
        # NPL Ratio (padrão de mercado - sobre carteira total)
        npl_ratio = MetricsCalculator.calculate_npl(creditos_vencidos_total, carteira_total)
        
        # NPL Ratio (alternativo - sobre créditos adquiridos)
        npl_ratio_credito_puro = MetricsCalculator.calculate_npl(
            creditos_vencidos_total, creditos_adquiridos
        )
        
        # Liquidez Imediata
        liquidez_imediata = MetricsCalculator.calculate_liquidez(
            disponibilidades, passivo_circulante
        )
        
        # Zumbi Ratio
        zumbi_ratio = MetricsCalculator.calculate_zumbi_ratio(
            aging_maior_1080, carteira_total
        )
        
        # Indicadores legados
        indice_npl = (inadimplencia_total / carteira_total * 100) if carteira_total > 0 else 0.0
        taxa_liquidez = (disponibilidades / ativo_total * 100) if ativo_total > 0 else 0.0
        concentracao_credito = (carteira_total / ativo_total * 100) if ativo_total > 0 else 0.0
        
        return FundMetrics(
            npl_ratio=npl_ratio,
            npl_ratio_credito_puro=npl_ratio_credito_puro,
            liquidez_imediata_ratio=liquidez_imediata,
            zumbi_ratio=zumbi_ratio,
            indice_npl_percentual=indice_npl,
            taxa_liquidez_percentual=taxa_liquidez,
            concentracao_credito_percentual=concentracao_credito,
            inadimplencia_total=inadimplencia_total
        )
    
    @staticmethod
    def calculate_npl(creditos_vencidos: float, base: float) -> float:
        """
        Calcula NPL Ratio (Non-Performing Loans).
        
        NPL = (Créditos Vencidos / Base) × 100
        
        Args:
            creditos_vencidos: Total de créditos vencidos inadimplentes
            base: Base de cálculo (carteira total ou créditos adquiridos)
            
        Returns:
            float: NPL em percentual (0-100+)
        """
        if base > 0 and creditos_vencidos >= 0:
            return (creditos_vencidos / base) * 100
        return 0.0
    
    @staticmethod
    def calculate_liquidez(disponibilidades: float, passivo_circulante: float) -> float:
        """
        Calcula Liquidez Imediata.
        
        Liquidez = Disponibilidades / Passivo Circulante
        
        Args:
            disponibilidades: Caixa e equivalentes
            passivo_circulante: Passivo de curto prazo
            
        Returns:
            float: Índice de liquidez (0-1+)
        """
        if passivo_circulante > 0:
            return disponibilidades / passivo_circulante
        return 0.0
    
    @staticmethod
    def calculate_zumbi_ratio(aging_1080_dias: float, carteira_total: float) -> float:
        """
        Calcula Zumbi Ratio (créditos muito antigos).
        
        Zumbi = (Créditos > 1080 dias / Carteira Total) × 100
        
        Args:
            aging_1080_dias: Créditos vencidos há mais de 3 anos
            carteira_total: Total da carteira
            
        Returns:
            float: Zumbi ratio em percentual
        """
        if carteira_total > 0 and aging_1080_dias >= 0:
            return (aging_1080_dias / carteira_total) * 100
        return 0.0
    
    @staticmethod
    def add_metrics_to_data(dados: Dict[str, Any]) -> None:
        """
        Calcula e adiciona métricas ao dicionário de dados.
        
        Args:
            dados: Dicionário a ser modificado in-place
        """
        metrics = MetricsCalculator.calculate_all_metrics(dados)
        
        dados['NPL_RATIO'] = metrics.npl_ratio
        dados['NPL_RATIO_CREDITO_PURO'] = metrics.npl_ratio_credito_puro
        dados['LIQUIDEZ_IMEDIATA_RATIO'] = metrics.liquidez_imediata_ratio
        dados['ZUMBI_RATIO'] = metrics.zumbi_ratio
        dados['INDICE_NPL_PERCENTUAL'] = metrics.indice_npl_percentual
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = metrics.taxa_liquidez_percentual
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = metrics.concentracao_credito_percentual
        dados['INADIMPLENCIA_TOTAL'] = metrics.inadimplencia_total
        
        # Flag para NPL outlier
        if metrics.npl_ratio > 1000:
            logger.warning(
                f"⚠️ NPL outlier: {metrics.npl_ratio:.2f}% | "
                f"CNPJ: {dados.get('CNPJ_FUNDO', 'N/A')}"
            )
            dados['NPL_OUTLIER_FLAG'] = True
        else:
            dados['NPL_OUTLIER_FLAG'] = False

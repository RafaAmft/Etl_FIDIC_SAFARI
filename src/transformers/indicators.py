"""
Calculadora de indicadores financeiros para FIDC.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import pandas as pd
import numpy as np
import logging


logger = logging.getLogger(__name__)


class IndicatorCalculator:
    """Calcula indicadores financeiros para FIDCs."""
    
    @staticmethod
    def calculate_liquidez(df: pd.DataFrame) -> pd.Series:
        """
        Calcula taxa de liquidez: DISPONIBILIDADES / ATIVO_TOTAL.
        
        Args:
            df: DataFrame com colunas DISPONIBILIDADES e ATIVO_TOTAL
            
        Returns:
            Series com liquidez calculada (formato decimal)
        """
        return np.where(
            df['ATIVO_TOTAL'] == 0,
            0,
            df['DISPONIBILIDADES'] / df['ATIVO_TOTAL']
        )
    
    @staticmethod
    def calculate_npl(df: pd.DataFrame) -> pd.Series:
        """
        Calcula NPL (Non-Performing Loans): INADIMPLENCIA_TOTAL / CARTEIRA_BRUTA.
        
        Args:
            df: DataFrame com colunas INADIMPLENCIA_TOTAL e CARTEIRA_BRUTA
            
        Returns:
            Series com NPL calculado (formato decimal)
        """
        return np.where(
            df['CARTEIRA_BRUTA'] == 0,
            0,
            df['INADIMPLENCIA_TOTAL'] / df['CARTEIRA_BRUTA']
        )
    
    @staticmethod
    def calculate_carteira_liquida(df: pd.DataFrame) -> pd.Series:
        """
        Calcula carteira líquida: CREDITOS_ADQUIRIDOS + DICRED_TOTAL.
        
        Args:
            df: DataFrame com colunas CREDITOS_ADQUIRIDOS e DICRED_TOTAL
            
        Returns:
            Series com carteira líquida calculada
        """
        return df['CREDITOS_ADQUIRIDOS'] + df['DICRED_TOTAL']
    
    @staticmethod
    def apply_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todos os indicadores ao DataFrame.
        
        Args:
            df: DataFrame com dados FIDC
            
        Returns:
            DataFrame com colunas de indicadores adicionadas
        """
        df_copy = df.copy()
        
        # Liquidez calculada
        df_copy['liquidez_calc'] = IndicatorCalculator.calculate_liquidez(df_copy)
        
        # NPL calculado
        df_copy['npl_calc'] = IndicatorCalculator.calculate_npl(df_copy)
        
        # Carteira líquida calculada
        df_copy['CARTEIRA_LIQUIDA_CALC'] = IndicatorCalculator.calculate_carteira_liquida(df_copy)
        
        logger.info("Indicadores calculados com sucesso")
        
        return df_copy

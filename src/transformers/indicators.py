"""
Calculadora de indicadores financeiros para FIDC.

ATENÇÃO — MÓDULO AUXILIAR (NÃO USADO EM PRODUÇÃO):
    O pipeline de produção calcula todos os indicadores diretamente em
    src/extractors/xml_parser.py (golden source), seguindo a lógica do
    notebook 'etl_fidic_vfinal.ipynb'. Este módulo foi removido do
    FIDCETLService e existe apenas como utilitário de análise avulsa.

    Denominadores de referência (golden source — xml_parser.py):
        NPL      → INADIMPLENCIA_TOTAL / CREDITOS_ADQUIRIDOS
        Liquidez → DISPONIBILIDADES    / ATIVO_TOTAL

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import warnings
import pandas as pd
import numpy as np
import logging


logger = logging.getLogger(__name__)


class IndicatorCalculator:
    """
    Calcula indicadores financeiros para FIDCs.

    .. deprecated::
        Este módulo não é mais utilizado pelo pipeline de produção.
        O golden source dos indicadores é src/extractors/xml_parser.py.
        Use esta classe apenas para análises avulsas, garantindo que os
        denominadores abaixo estejam alinhados com o parser.
    """

    @staticmethod
    def calculate_liquidez(df: pd.DataFrame) -> pd.Series:
        """
        Calcula taxa de liquidez: DISPONIBILIDADES / ATIVO_TOTAL.

        Alinhado com golden source (xml_parser.py:246).

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
        Calcula NPL (Non-Performing Loans): INADIMPLENCIA_TOTAL / CREDITOS_ADQUIRIDOS.

        Denominador: CREDITOS_ADQUIRIDOS — alinhado com golden source (xml_parser.py:234).

        ATENÇÃO: versões anteriores usavam CARTEIRA_BRUTA como denominador, o que
        gerava NPL subestimado em fundos com DICRED_TOTAL > 0, pois CARTEIRA_BRUTA
        poderia ser interpretado como CREDITOS_ADQUIRIDOS + DICRED_TOTAL.

        Args:
            df: DataFrame com colunas INADIMPLENCIA_TOTAL e CREDITOS_ADQUIRIDOS

        Returns:
            Series com NPL calculado (formato decimal, 0–1)
        """
        return np.where(
            df['CREDITOS_ADQUIRIDOS'] == 0,
            0,
            df['INADIMPLENCIA_TOTAL'] / df['CREDITOS_ADQUIRIDOS']
        )

    @staticmethod
    def calculate_total_creditos(df: pd.DataFrame) -> pd.Series:
        """
        Calcula o total consolidado de créditos: CREDITOS_ADQUIRIDOS + DICRED_TOTAL.

        Representa a soma dos dois nós de crédito do XML (CRED_EXISTE + DICRED).
        NÃO deve ser usado como denominador do NPL — ver calculate_npl().

        Args:
            df: DataFrame com colunas CREDITOS_ADQUIRIDOS e DICRED_TOTAL

        Returns:
            Series com total de créditos consolidado
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
        warnings.warn(
            "IndicatorCalculator não é usado pelo pipeline de produção. "
            "O golden source é xml_parser.py. Use com cautela.",
            DeprecationWarning,
            stacklevel=2,
        )

        df_copy = df.copy()

        df_copy['liquidez_calc'] = IndicatorCalculator.calculate_liquidez(df_copy)
        df_copy['npl_calc'] = IndicatorCalculator.calculate_npl(df_copy)
        df_copy['TOTAL_CREDITOS_CALC'] = IndicatorCalculator.calculate_total_creditos(df_copy)

        logger.info("Indicadores calculados com sucesso")

        return df_copy

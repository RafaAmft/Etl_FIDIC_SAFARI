"""
Validador de dados FIDC com flags de Quality Assurance.

Adaptado para a nova lógica "Golden Source" do notebook:
- Removeu validações de divergência (pois agora só temos o valor calculado)
- Atualizou nomes de colunas para o novo modelo

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import pandas as pd
import numpy as np
import logging

from ..config import settings


logger = logging.getLogger(__name__)


class FIDCValidator:
    """Validador de dados FIDC com flags de QA."""
    
    @staticmethod
    def validate_ativo_zero(df: pd.DataFrame) -> pd.Series:
        """
        Identifica fundos com ATIVO_TOTAL = 0.
        """
        return df['ATIVO_TOTAL'] == 0
    
    @staticmethod
    def validate_carteira_bruta_zero_com_inad(df: pd.DataFrame) -> pd.Series:
        """
        Identifica fundos com CARTEIRA_BRUTA = 0 mas INADIMPLENCIA_TOTAL > 0.
        Case anômalo.
        """
        return (df['CARTEIRA_BRUTA'] == 0) & (df['INADIMPLENCIA_TOTAL'] > 0)
    
    @staticmethod
    def validate_sem_posicao(df: pd.DataFrame) -> pd.Series:
        """
        Identifica fundos sem posição de crédito mas com ativo.
        
        CARTEIRA_TOTAL = 0 e ATIVO_TOTAL > 0
        """
        return (df['CARTEIRA_TOTAL'] == 0) & (df['ATIVO_TOTAL'] > 0)
    
    @staticmethod
    def apply_all_validations(df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas as validações ao DataFrame.
        
        Adiciona 3 flags principais de QA:
        - ATIVO_ZERO_FLAG
        - CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG
        - SEM_POSICAO_FLAG
        """
        df_copy = df.copy()
        
        # Aplicar validações compatíveis com novo modelo
        df_copy['ATIVO_ZERO_FLAG'] = FIDCValidator.validate_ativo_zero(df_copy)
        df_copy['CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG'] = FIDCValidator.validate_carteira_bruta_zero_com_inad(df_copy)
        df_copy['SEM_POSICAO_FLAG'] = FIDCValidator.validate_sem_posicao(df_copy)
        
        # Log de resumo
        total_issues = df_copy[[
            'ATIVO_ZERO_FLAG',
            'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG',
            'SEM_POSICAO_FLAG'
        ]].sum()
        
        logger.info("Validações aplicadas:")
        for flag, count in total_issues.items():
            logger.info(f"  {flag}: {count} registros")
        
        return df_copy
    
    @staticmethod
    def filter_qa_issues(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra apenas registros com alguma flag ativa."""
        cols_to_check = [
            'ATIVO_ZERO_FLAG',
            'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG',
            'SEM_POSICAO_FLAG'
        ]
        
        # Garantir que colunas existem
        existing_cols = [c for c in cols_to_check if c in df.columns]
        
        if not existing_cols:
            return pd.DataFrame()

        has_issues = df[existing_cols].any(axis=1)
        return df[has_issues].copy()

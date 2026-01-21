"""
Gerador de relatório de diferenças entre versões de dados FIDC.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional


logger = logging.getLogger(__name__)


class DiffGenerator:
    """Gera relatórios de diferenças entre duas versões de dados FIDC."""
    
    @staticmethod
    def generate_diff_report(
        df_v1: pd.DataFrame,
        df_v2: pd.DataFrame,
        output_file: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Compara colunas numéricas entre dois DataFrames para CNPJs comuns.
        
        Args:
            df_v1: DataFrame com versão 1 dos dados
            df_v2: DataFrame com versão 2 dos dados
            output_file: Caminho opcional para salvar o relatório em CSV
            
        Returns:
            DataFrame com diferenças encontradas (CNPJ, COLUNA, DIFERENCA)
        """
        # Garantir que CNPJ_FUNDO seja o índice
        df_v1_indexed = df_v1.set_index('CNPJ_FUNDO') if 'CNPJ_FUNDO' in df_v1.columns else df_v1
        df_v2_indexed = df_v2.set_index('CNPJ_FUNDO') if 'CNPJ_FUNDO' in df_v2.columns else df_v2
        
        # Identificar CNPJs comuns
        common_cnpjs = list(set(df_v1_indexed.index) & set(df_v2_indexed.index))
        
        if not common_cnpjs:
            logger.warning("Nenhum CNPJ comum encontrado entre as duas versões")
            return pd.DataFrame(columns=['CNPJ_FUNDO', 'COLUNA', 'DIFERENCA'])
        
        logger.info(f"Encontrados {len(common_cnpjs)} CNPJs comuns")
        
        # Filtrar para CNPJs comuns
        df_v1_common = df_v1_indexed.loc[common_cnpjs]
        df_v2_common = df_v2_indexed.loc[common_cnpjs]
        
        # Identificar colunas numéricas comuns
        numeric_cols_v1 = df_v1_common.select_dtypes(include=np.number).columns
        numeric_cols_v2 = df_v2_common.select_dtypes(include=np.number).columns
        common_numeric_cols = list(set(numeric_cols_v1) & set(numeric_cols_v2))
        
        if not common_numeric_cols:
            logger.warning("Nenhuma coluna numérica comum encontrada")
            return pd.DataFrame(columns=['CNPJ_FUNDO', 'COLUNA', 'DIFERENCA'])
        
        logger.info(f"Comparando {len(common_numeric_cols)} colunas numéricas")
        
        # Coletar diferenças
        diff_records = []
        
        for cnpj in common_cnpjs:
            for col in common_numeric_cols:
                val_v1 = df_v1_common.loc[cnpj, col]
                val_v2 = df_v2_common.loc[cnpj, col]
                
                # Calcular diferença tratando NaNs
                if pd.isna(val_v1) and pd.isna(val_v2):
                    continue  # Ambos NaN, sem diferença
                elif pd.isna(val_v1):
                    diff = val_v2
                elif pd.isna(val_v2):
                    diff = -val_v1
                else:
                    diff = val_v2 - val_v1
                
                # Adicionar apenas se diferença não-zero
                if diff != 0:
                    diff_records.append({
                        'CNPJ_FUNDO': cnpj,
                        'COLUNA': col,
                        'DIFERENCA': diff
                    })
        
        # Criar DataFrame de diferenças
        df_diff = pd.DataFrame(diff_records)
        
        logger.info(f"Encontradas {len(df_diff)} diferenças numéricas")
        
        # Salvar se output_file fornecido
        if output_file and not df_diff.empty:
            from ..config import settings
            df_diff.to_csv(
                output_file,
                index=False,
                sep=settings.EXPORT_SEP,
                decimal=settings.EXPORT_DECIMAL,
                float_format=settings.FLOAT_FORMAT,
                encoding=settings.EXPORT_ENCODING
            )
            logger.info(f"Relatório de diferenças salvo em: {output_file}")
        
        return df_diff

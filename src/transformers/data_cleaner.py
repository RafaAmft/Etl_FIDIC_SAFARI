"""
Utilitários para limpeza e transformação de dados.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

from datetime import datetime
import pandas as pd
import logging


logger = logging.getLogger(__name__)


def clean_cnpj(cnpj_str: str) -> str:
    """
    Limpa uma string de CNPJ removendo caracteres não numéricos
    e preenchendo com zeros à esquerda para garantir 14 dígitos.
    
    Examples:
        "51.199.121/0001-45" → "51199121000145"
        "51199121000145.0"   → "51199121000145"
        "123456"             → "00000000123456"
        
    Args:
        cnpj_str: String contendo o CNPJ
        
    Returns:
        CNPJ limpo com 14 dígitos
    """
    if not isinstance(cnpj_str, str):
        cnpj_str = str(cnpj_str)
    
    # Remove todos os caracteres não numéricos
    cleaned = ''.join(filter(str.isdigit, cnpj_str))
    
    # Garante 14 dígitos, pegando os primeiros 14 se maior
    # e preenchendo com zeros à esquerda se menor
    return cleaned[:14].zfill(14)


def convert_pt_br_to_float(value: str) -> float:
    """
    Converte valor no formato brasileiro para float.
    Lida com separadores de milhar (.) e decimal (,).
    
    Examples:
        "1.234.567,89" → 1234567.89
        "123,45"       → 123.45
        "-"            → 0.0
        ""             → 0.0
        
    Args:
        value: String com número no formato PT-BR
        
    Returns:
        Valor convertido para float
    """
    if not value or not str(value).strip():
        return 0.0
    
    # Substitui '-' por '0'
    value_str = str(value).strip()
    if value_str == '-':
        return 0.0
    
    try:
        # Remove pontos e troca vírgula por ponto
        cleaned = value_str.replace('.', '').replace(',', '.')
        return float(cleaned)
    except (ValueError, AttributeError):
        return 0.0


def convert_data_referencia(date_str: str) -> datetime:
    """
    Converte string de data no formato MM/YYYY para datetime.
    
    Examples:
        "11/2025" → datetime(2025, 11, 1)
        "01/2024" → datetime(2024, 1, 1)
        
    Args:
        date_str: String no formato MM/YYYY
        
    Returns:
        Objeto datetime (primeiro dia do mês)
    """
    try:
        return pd.to_datetime(date_str, format='%m/%Y', errors='coerce')
    except:
        return pd.NaT


def convert_numeric_columns(
    df: pd.DataFrame,
    columns: list,
    replace_dash: bool = True
) -> pd.DataFrame:
    """
    Converte múltiplas colunas para tipo numérico.
    
    Args:
        df: DataFrame a ser transformado
        columns: Lista de nomes de colunas
        replace_dash: Se True, substitui '-' por '0' antes da conversão
        
    Returns:
        DataFrame com colunas convertidas
    """
    df_copy = df.copy()
    
    for col in columns:
        if col in df_copy.columns:
            # Se já é numérico, apenas garantir float e pular processamento de string
            # Isso evita que números em notação científica (ex: 1.23e-10) virem strings
            # e tenham o '-' do expoente substituído incorretamente.
            if pd.api.types.is_numeric_dtype(df_copy[col]):
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                continue
            
            # Garantir string apenas para colunas object
            df_copy[col] = df_copy[col].astype(str)
            
            if replace_dash:
                # Substituir APENAS células que são exatamente '-' ou espaços
                # IMPORTANTE: Não usar str.replace('-', '0') pois isso corrompe "1.23e-10" -> "1.23e010"
                df_copy[col] = df_copy[col].replace(r'^\s*-\s*$', '0', regex=True)
            
            # Converter para numérico
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
        else:
            logger.warning(f"Coluna '{col}' não encontrada no DataFrame")
    
    return df_copy

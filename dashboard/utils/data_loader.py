"""
Carregador de dados para o dashboard FIDC Monitor.

Implementa carregamento com cache, validação e tratamento de erros.
"""
import pandas as pd
import streamlit as st
import logging
from typing import Tuple, List, Optional
from pathlib import Path

from config.settings import DEFAULT_DATA_FILE, REQUIRED_COLUMNS

logger = logging.getLogger(__name__)


@st.cache_data(ttl=3600, show_spinner="📊 Carregando dados...")
def load_data(filepath: Optional[str] = None) -> pd.DataFrame:
    """
    Carrega dados do CSV com cache de 1 hora.
    
    Args:
        filepath: Caminho do arquivo. Se None, usa DEFAULT_DATA_FILE.
        
    Returns:
        DataFrame com os dados carregados.
        
    Raises:
        FileNotFoundError: Se o arquivo não existir.
        ValueError: Se o arquivo estiver vazio ou mal formatado.
    """
    if filepath is None:
        filepath = str(DEFAULT_DATA_FILE)
    
    path = Path(filepath)
    
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")
    
    # Detectar separador (CSV brasileiro usa ;)
    df = pd.read_csv(filepath, sep=';', decimal=',', encoding='utf-8-sig')
    
    if df.empty:
        raise ValueError("Arquivo CSV está vazio")
    
    # Converter DATA_COMPETENCIA para datetime se possível
    if 'DATA_COMPETENCIA' in df.columns:
        df['DATA_COMPETENCIA'] = df['DATA_COMPETENCIA'].astype(str)
    
    # Garantir tipos numéricos
    numeric_cols = [
        'ATIVO_TOTAL', 'CARTEIRA_TOTAL', 'INADIMPLENCIA_TOTAL',
        'INDICE_NPL_DECIMAL', 'TAXA_LIQUIDEZ_DECIMAL', 'CARTEIRA_BRUTA'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    logger.info(f"✅ Dados carregados: {len(df)} registros, {len(df.columns)} colunas")
    
    return df


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Valida se o DataFrame possui estrutura mínima esperada.
    
    Args:
        df: DataFrame a validar.
        
    Returns:
        Tuple (is_valid, list_of_errors)
    """
    errors = []
    
    if df.empty:
        errors.append("DataFrame está vazio")
        return False, errors
    
    # Verificar colunas obrigatórias
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        errors.append(f"Colunas obrigatórias faltando: {', '.join(missing)}")
    
    # Verificar se há dados de sucesso
    if 'STATUS' in df.columns:
        success_count = (df['STATUS'] == 'SUCESSO').sum()
        if success_count == 0:
            errors.append("Nenhum registro com STATUS='SUCESSO'")
    
    return len(errors) == 0, errors


def get_success_only(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra apenas registros com STATUS='SUCESSO'."""
    if 'STATUS' in df.columns:
        return df[df['STATUS'] == 'SUCESSO'].copy()
    return df.copy()


def get_unique_periods(df: pd.DataFrame) -> List[str]:
    """Retorna lista de períodos únicos ordenados."""
    if 'DATA_COMPETENCIA' not in df.columns:
        return []
    
    periods = df['DATA_COMPETENCIA'].dropna().unique().tolist()
    # Ordenar por ano/mês (formato esperado: MM/YYYY ou YYYY-MM)
    try:
        periods = sorted(periods, reverse=True)
    except:
        pass
    return periods


def get_unique_cnpjs(df: pd.DataFrame) -> List[str]:
    """Retorna lista de CNPJs únicos."""
    if 'CNPJ_FUNDO' not in df.columns:
        return []
    return sorted(df['CNPJ_FUNDO'].dropna().unique().tolist())


def get_segment_columns(df: pd.DataFrame) -> List[str]:
    """Retorna lista de colunas de segmentação."""
    return [col for col in df.columns if col.startswith('SEGMT_')]


def get_last_update_info(df: pd.DataFrame) -> dict:
    """Retorna informações sobre a última atualização."""
    info = {
        'total_fundos': len(df['CNPJ_FUNDO'].unique()) if 'CNPJ_FUNDO' in df.columns else 0,
        'total_registros': len(df),
        'ultima_competencia': None,
        'total_sucesso': 0,
        'total_erros': 0
    }
    
    if 'DATA_COMPETENCIA' in df.columns:
        periods = get_unique_periods(df)
        info['ultima_competencia'] = periods[0] if periods else None
    
    if 'STATUS' in df.columns:
        info['total_sucesso'] = (df['STATUS'] == 'SUCESSO').sum()
        info['total_erros'] = (df['STATUS'] != 'SUCESSO').sum()
    
    return info

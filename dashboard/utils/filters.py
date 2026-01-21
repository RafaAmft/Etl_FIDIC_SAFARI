"""
Componentes de filtro reutilizáveis para o dashboard FIDC Monitor.
"""
import streamlit as st
import pandas as pd
from typing import List, Optional, Tuple


def create_period_filter(
    df: pd.DataFrame,
    key: str = "period_filter",
    label: str = "📅 Período",
    default_all: bool = True
) -> List[str]:
    """
    Cria filtro de seleção de período.
    
    Args:
        df: DataFrame com coluna DATA_COMPETENCIA
        key: Chave única do widget
        label: Rótulo do filtro
        default_all: Se True, seleciona todos por padrão
        
    Returns:
        Lista de períodos selecionados
    """
    if 'DATA_COMPETENCIA' not in df.columns:
        return []
    
    periods = sorted(df['DATA_COMPETENCIA'].dropna().unique().tolist(), reverse=True)
    
    if not periods:
        return []
    
    default = periods if default_all else [periods[0]]
    
    selected = st.multiselect(
        label,
        options=periods,
        default=default,
        key=key
    )
    
    return selected


def create_status_filter(
    df: pd.DataFrame,
    key: str = "status_filter",
    label: str = "📋 Status"
) -> List[str]:
    """
    Cria filtro de seleção de status.
    """
    if 'STATUS' not in df.columns:
        return []
    
    statuses = df['STATUS'].dropna().unique().tolist()
    
    selected = st.multiselect(
        label,
        options=statuses,
        default=['SUCESSO'] if 'SUCESSO' in statuses else statuses,
        key=key
    )
    
    return selected


def create_segment_filter(
    df: pd.DataFrame,
    key: str = "segment_filter",
    label: str = "🏢 Segmentos"
) -> List[str]:
    """
    Cria filtro de seleção de segmentos.
    """
    segment_cols = [col for col in df.columns if col.startswith('SEGMT_')]
    
    if not segment_cols:
        return []
    
    # Criar nomes amigáveis
    segments = {
        col.replace('SEGMT_', '').replace('_', ' ').title(): col 
        for col in segment_cols
    }
    
    selected_names = st.multiselect(
        label,
        options=list(segments.keys()),
        key=key
    )
    
    # Retornar nomes das colunas originais
    return [segments[name] for name in selected_names]


def create_asset_range_filter(
    df: pd.DataFrame,
    key: str = "asset_filter",
    label: str = "💰 Faixa de Ativo Total"
) -> Tuple[float, float]:
    """
    Cria filtro de range de ativo total.
    
    Returns:
        Tuple (min_value, max_value)
    """
    if 'ATIVO_TOTAL' not in df.columns:
        return (0, 0)
    
    min_val = float(df['ATIVO_TOTAL'].min())
    max_val = float(df['ATIVO_TOTAL'].max())
    
    if min_val == max_val:
        return (min_val, max_val)
    
    # Usar slider com formatação em milhões
    values = st.slider(
        label,
        min_value=min_val,
        max_value=max_val,
        value=(min_val, max_val),
        format="R$ %.0f",
        key=key
    )
    
    return values


def create_npl_range_filter(
    df: pd.DataFrame,
    key: str = "npl_filter",
    label: str = "📈 Faixa de NPL (%)"
) -> Tuple[float, float]:
    """
    Cria filtro de range de NPL.
    
    Returns:
        Tuple (min_percent, max_percent)
    """
    if 'INDICE_NPL_DECIMAL' not in df.columns:
        return (0.0, 100.0)
    
    # Converter para percentual para exibição
    min_val = 0.0
    max_val = min(100.0, float(df['INDICE_NPL_DECIMAL'].max()) * 100)
    
    values = st.slider(
        label,
        min_value=min_val,
        max_value=max(max_val, 100.0),
        value=(min_val, max_val),
        step=1.0,
        format="%.0f%%",
        key=key
    )
    
    return values


def create_fund_search(
    df: pd.DataFrame,
    key: str = "fund_search",
    label: str = "🔍 Buscar Fundo (CNPJ)"
) -> Optional[str]:
    """
    Cria campo de busca de fundo por CNPJ.
    
    Returns:
        CNPJ selecionado ou None
    """
    if 'CNPJ_FUNDO' not in df.columns:
        return None
    
    cnpjs = sorted(df['CNPJ_FUNDO'].dropna().unique().tolist())
    
    if not cnpjs:
        return None
    
    selected = st.selectbox(
        label,
        options=[''] + cnpjs,
        index=0,
        key=key,
        format_func=lambda x: "Selecione um fundo..." if x == '' else x
    )
    
    return selected if selected else None


def apply_period_filter(df: pd.DataFrame, periods: List[str]) -> pd.DataFrame:
    """Aplica filtro de período ao DataFrame."""
    if not periods or 'DATA_COMPETENCIA' not in df.columns:
        return df
    
    return df[df['DATA_COMPETENCIA'].isin(periods)].copy()


def apply_status_filter(df: pd.DataFrame, statuses: List[str]) -> pd.DataFrame:
    """Aplica filtro de status ao DataFrame."""
    if not statuses or 'STATUS' not in df.columns:
        return df
    
    return df[df['STATUS'].isin(statuses)].copy()


def apply_asset_filter(df: pd.DataFrame, min_val: float, max_val: float) -> pd.DataFrame:
    """Aplica filtro de ativo total ao DataFrame."""
    if 'ATIVO_TOTAL' not in df.columns:
        return df
    
    return df[
        (df['ATIVO_TOTAL'] >= min_val) & 
        (df['ATIVO_TOTAL'] <= max_val)
    ].copy()


def apply_npl_filter(df: pd.DataFrame, min_pct: float, max_pct: float) -> pd.DataFrame:
    """Aplica filtro de NPL ao DataFrame (valores em %)."""
    if 'INDICE_NPL_DECIMAL' not in df.columns:
        return df
    
    # Converter % para decimal
    min_dec = min_pct / 100
    max_dec = max_pct / 100
    
    return df[
        (df['INDICE_NPL_DECIMAL'] >= min_dec) & 
        (df['INDICE_NPL_DECIMAL'] <= max_dec)
    ].copy()


def create_sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria grupo de filtros padrão na sidebar e retorna DataFrame filtrado.
    
    Returns:
        DataFrame após aplicação de todos os filtros
    """
    st.sidebar.header("🎛️ Filtros")
    
    df_filtered = df.copy()
    
    # Filtro de status
    statuses = create_status_filter(df, key="sidebar_status")
    if statuses:
        df_filtered = apply_status_filter(df_filtered, statuses)
    
    # Filtro de período
    periods = create_period_filter(df_filtered, key="sidebar_period")
    if periods:
        df_filtered = apply_period_filter(df_filtered, periods)
    
    # Info sobre filtros aplicados
    st.sidebar.markdown("---")
    st.sidebar.caption(f"📊 {len(df_filtered):,} registros após filtros")
    
    return df_filtered

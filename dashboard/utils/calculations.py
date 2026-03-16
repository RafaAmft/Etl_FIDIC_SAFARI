"""
Funções de cálculo e métricas para o dashboard FIDC Monitor.
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any


def calculate_completeness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula taxa de completude (preenchimento) por coluna.
    
    Returns:
        DataFrame com colunas: coluna, total, preenchidos, percentual
    """
    results = []
    total_rows = len(df)
    
    for col in df.columns:
        non_null = df[col].notna().sum()
        # Para numéricos, considerar 0 como preenchido (é válido)
        if df[col].dtype in ['float64', 'int64']:
            non_null = total_rows - df[col].isna().sum()
        
        pct = (non_null / total_rows * 100) if total_rows > 0 else 0
        
        results.append({
            'coluna': col,
            'total': total_rows,
            'preenchidos': non_null,
            'percentual': round(pct, 2)
        })
    
    return pd.DataFrame(results).sort_values('percentual', ascending=True)


def calculate_flag_summary(df: pd.DataFrame, flag_columns: List[str]) -> pd.DataFrame:
    """
    Calcula resumo de flags de QA.
    
    Args:
        df: DataFrame com dados
        flag_columns: Lista de colunas de flags
        
    Returns:
        DataFrame com: flag, quantidade, percentual
    """
    results = []
    total = len(df)
    
    for col in flag_columns:
        if col not in df.columns:
            continue
        
        # Converter para bool se necessário
        try:
            count = df[col].astype(bool).sum()
        except:
            count = (df[col] == True).sum()
        
        pct = (count / total * 100) if total > 0 else 0
        
        # Nome amigável
        friendly_name = col.replace('_FLAG', '').replace('_', ' ').title()
        
        results.append({
            'flag': friendly_name,
            'coluna_original': col,
            'quantidade': int(count),
            'percentual': round(pct, 2)
        })
    
    return pd.DataFrame(results).sort_values('quantidade', ascending=False)


def calculate_npl_metrics(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calcula métricas estatísticas de NPL.
    
    Returns:
        Dicionário com métricas: media, mediana, std, min, max, q25, q75
    """
    if 'INDICE_NPL_DECIMAL' not in df.columns:
        return {}
    
    npl = df['INDICE_NPL_DECIMAL'].dropna()
    
    if len(npl) == 0:
        return {}
    
    return {
        'media': float(npl.mean()),
        'mediana': float(npl.median()),
        'std': float(npl.std()),
        'min': float(npl.min()),
        'max': float(npl.max()),
        'q25': float(npl.quantile(0.25)),
        'q75': float(npl.quantile(0.75))
    }


def calculate_volume_by_segment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula volume total por segmento.
    
    Returns:
        DataFrame com: segmento, volume, percentual
    """
    segment_cols = [col for col in df.columns if col.startswith('SEGMT_')]
    
    if not segment_cols:
        return pd.DataFrame()
    
    results = []
    total_volume = 0
    
    for col in segment_cols:
        if col in df.columns:
            volume = df[col].sum()
            if volume > 0:
                segment_name = col.replace('SEGMT_', '').replace('_', ' ').title()
                results.append({
                    'segmento': segment_name,
                    'coluna': col,
                    'volume': volume
                })
                total_volume += volume
    
    df_result = pd.DataFrame(results)
    
    if not df_result.empty and total_volume > 0:
        df_result['percentual'] = (df_result['volume'] / total_volume * 100).round(2)
        df_result = df_result.sort_values('volume', ascending=False)
    
    return df_result


def calculate_temporal_evolution(
    df: pd.DataFrame, 
    value_column: str,
    date_column: str = 'DATA_COMPETENCIA',
    agg_func: str = 'sum'
) -> pd.DataFrame:
    """
    Calcula evolução temporal de uma métrica.
    
    Args:
        df: DataFrame com dados
        value_column: Coluna a agregar
        date_column: Coluna de data
        agg_func: Função de agregação ('sum', 'mean', 'count')
        
    Returns:
        DataFrame com: periodo, valor
    """
    if date_column not in df.columns or value_column not in df.columns:
        return pd.DataFrame()
    
    grouped = df.groupby(date_column)[value_column].agg(agg_func).reset_index()
    grouped.columns = ['periodo', 'valor']
    
    # Tentar ordenar por período
    try:
        grouped = grouped.sort_values('periodo')
    except:
        pass
    
    return grouped


def calculate_distribution(
    df: pd.DataFrame, 
    column: str,
    bins: int = 20
) -> Dict[str, Any]:
    """
    Calcula distribuição de uma coluna numérica.
    
    Returns:
        Dicionário com: values, bin_edges, histogram
    """
    if column not in df.columns:
        return {}
    
    values = df[column].dropna()
    
    if len(values) == 0:
        return {}
    
    hist, bin_edges = np.histogram(values, bins=bins)
    
    return {
        'values': values.tolist(),
        'histogram': hist.tolist(),
        'bin_edges': bin_edges.tolist(),
        'stats': {
            'count': len(values),
            'mean': float(values.mean()),
            'std': float(values.std()),
            'min': float(values.min()),
            'max': float(values.max())
        }
    }


def calculate_fund_metrics(df: pd.DataFrame, cnpj: str) -> Dict[str, Any]:
    """
    Calcula métricas para um fundo específico.
    
    Args:
        df: DataFrame com dados
        cnpj: CNPJ do fundo
        
    Returns:
        Dicionário com métricas do fundo
    """
    if 'CNPJ_FUNDO' not in df.columns:
        return {}
    
    fund_data = df[df['CNPJ_FUNDO'] == cnpj]
    
    if fund_data.empty:
        return {}
    
    # Último registro
    latest = fund_data.iloc[-1] if len(fund_data) > 0 else None
    
    metrics = {
        'cnpj': cnpj,
        'total_registros': len(fund_data),
        'periodos': fund_data['DATA_COMPETENCIA'].unique().tolist() if 'DATA_COMPETENCIA' in fund_data.columns else []
    }
    
    if latest is not None:
        metrics.update({
            'ativo_total': float(latest.get('ATIVO_TOTAL', 0)),
            'carteira_total': float(latest.get('CARTEIRA_TOTAL', 0)),
            'inadimplencia': float(latest.get('INADIMPLENCIA_TOTAL', 0)),
            'npl': float(latest.get('INDICE_NPL_DECIMAL', 0)),
            'liquidez': float(latest.get('TAXA_LIQUIDEZ_DECIMAL', 0)) * 100,
            'status': str(latest.get('STATUS', 'N/A'))
        })
    
    return metrics


def format_currency(value: float) -> str:
    """Formata valor como moeda brasileira."""
    if pd.isna(value) or value == 0:
        return "R$ 0,00"
    
    if abs(value) >= 1e9:
        return f"R$ {value/1e9:,.2f}B"
    elif abs(value) >= 1e6:
        return f"R$ {value/1e6:,.2f}M"
    elif abs(value) >= 1e3:
        return f"R$ {value/1e3:,.2f}K"
    else:
        return f"R$ {value:,.2f}"


def format_percent(value: float, decimal: bool = False) -> str:
    """Formata valor como percentual."""
    if pd.isna(value):
        return "0,00%"
    
    # Se não é decimal (já é percentual), não multiplica
    if not decimal:
        return f"{value:.2f}%"
    
    return f"{value * 100:.2f}%"

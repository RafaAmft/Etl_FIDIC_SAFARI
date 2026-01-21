"""
Funções de visualização para o dashboard FIDC Monitor.

Gráficos Plotly padronizados e componentes de exibição.
"""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
from typing import Optional, List, Dict, Any

from config.settings import PLOTLY_CONFIG, PLOTLY_LAYOUT


def create_line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    height: int = 400
) -> go.Figure:
    """
    Cria gráfico de linha padronizado.
    
    Args:
        df: DataFrame com dados
        x: Coluna para eixo X
        y: Coluna para eixo Y
        title: Título do gráfico
        color: Coluna para cores (opcional)
        labels: Dicionário de labels customizados
        height: Altura do gráfico
    """
    fig = px.line(
        df, 
        x=x, 
        y=y, 
        title=title,
        color=color,
        labels=labels,
        markers=True
    )
    
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    fig.update_traces(line=dict(width=2))
    
    return fig


def create_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    orientation: str = 'v',
    color: Optional[str] = None,
    labels: Optional[Dict[str, str]] = None,
    height: int = 400,
    text_auto: bool = False
) -> go.Figure:
    """
    Cria gráfico de barras padronizado.
    
    Args:
        orientation: 'v' para vertical, 'h' para horizontal
    """
    fig = px.bar(
        df,
        x=x,
        y=y,
        title=title,
        orientation=orientation,
        color=color,
        labels=labels,
        text_auto=text_auto
    )
    
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    
    return fig


def create_pie_chart(
    df: pd.DataFrame,
    names: str,
    values: str,
    title: str,
    hole: float = 0.4,
    height: int = 400
) -> go.Figure:
    """
    Cria gráfico de pizza/donut padronizado.
    
    Args:
        hole: 0 para pizza, 0.4 para donut
    """
    fig = px.pie(
        df,
        names=names,
        values=values,
        title=title,
        hole=hole
    )
    
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig


def create_histogram(
    df: pd.DataFrame,
    x: str,
    title: str,
    nbins: int = 30,
    height: int = 400,
    color: Optional[str] = None
) -> go.Figure:
    """Cria histograma padronizado."""
    fig = px.histogram(
        df,
        x=x,
        title=title,
        nbins=nbins,
        color=color
    )
    
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    
    return fig


def create_scatter(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: Optional[str] = None,
    size: Optional[str] = None,
    hover_data: Optional[List[str]] = None,
    height: int = 400
) -> go.Figure:
    """Cria gráfico de dispersão padronizado."""
    fig = px.scatter(
        df,
        x=x,
        y=y,
        title=title,
        color=color,
        size=size,
        hover_data=hover_data
    )
    
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    
    return fig


def create_treemap(
    df: pd.DataFrame,
    path: List[str],
    values: str,
    title: str,
    height: int = 500
) -> go.Figure:
    """Cria treemap padronizado."""
    fig = px.treemap(
        df,
        path=path,
        values=values,
        title=title
    )
    
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    
    return fig


def display_kpi_card(
    label: str,
    value: Any,
    delta: Optional[float] = None,
    delta_color: str = "normal",
    help_text: Optional[str] = None
):
    """
    Exibe um card de KPI usando st.metric.
    
    Args:
        label: Rótulo do KPI
        value: Valor formatado como string
        delta: Variação percentual (opcional)
        delta_color: 'normal', 'inverse', ou 'off'
        help_text: Texto de ajuda (opcional)
    """
    if delta is not None:
        st.metric(
            label=label,
            value=value,
            delta=f"{delta:+.2f}%",
            delta_color=delta_color,
            help=help_text
        )
    else:
        st.metric(
            label=label,
            value=value,
            help=help_text
        )


def display_kpi_row(kpis: List[Dict[str, Any]]):
    """
    Exibe uma linha de KPIs.
    
    Args:
        kpis: Lista de dicionários com keys: label, value, delta (opcional)
    """
    cols = st.columns(len(kpis))
    
    for col, kpi in zip(cols, kpis):
        with col:
            display_kpi_card(
                label=kpi.get('label', ''),
                value=kpi.get('value', ''),
                delta=kpi.get('delta'),
                help_text=kpi.get('help')
            )


def display_dataframe_styled(
    df: pd.DataFrame,
    height: int = 400,
    use_container_width: bool = True
):
    """
    Exibe DataFrame com estilização padrão.
    """
    st.dataframe(
        df,
        height=height,
        use_container_width=use_container_width,
        hide_index=True
    )


def render_plotly_chart(fig: go.Figure, use_container_width: bool = True):
    """
    Renderiza gráfico Plotly com configurações padrão.
    """
    st.plotly_chart(
        fig,
        use_container_width=use_container_width,
        config=PLOTLY_CONFIG
    )


def create_completeness_chart(df_completeness: pd.DataFrame, top_n: int = 20) -> go.Figure:
    """
    Cria gráfico de barras horizontais para completude.
    
    Args:
        df_completeness: DataFrame com colunas: coluna, percentual
        top_n: Número de colunas a mostrar
    """
    # Pegar as N piores (menor completude)
    df_show = df_completeness.head(top_n).copy()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_show['percentual'],
        y=df_show['coluna'],
        orientation='h',
        marker=dict(
            color=df_show['percentual'],
            colorscale='RdYlGn',
            cmin=0,
            cmax=100
        ),
        text=[f"{p:.1f}%" for p in df_show['percentual']],
        textposition='outside'
    ))
    
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title="📊 Completude por Coluna (Piores)",
        xaxis_title="Completude (%)",
        yaxis_title="",
        height=max(300, len(df_show) * 25),
        xaxis=dict(range=[0, 110])
    )
    
    return fig

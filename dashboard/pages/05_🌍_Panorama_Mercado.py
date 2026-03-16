"""
🌍 Panorama do Mercado - Visão Consolidada de FIDCs

Esta página apresenta visão macro do mercado de FIDCs brasileiro.
"""
import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import (
    load_data,
    get_success_only,
    calculate_npl_metrics,
    calculate_volume_by_segment,
    calculate_temporal_evolution,
    format_currency,
    format_percent,
    create_line_chart,
    create_bar_chart,
    create_pie_chart,
    create_histogram,
    render_plotly_chart,
    create_sidebar_filters,
    create_asset_range_filter,
    create_npl_range_filter,
    apply_asset_filter,
    apply_npl_filter
)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Panorama do Mercado | FIDC Monitor",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Panorama do Mercado")
st.markdown("### Visão Consolidada do Mercado de FIDCs")

# ═══════════════════════════════════════════════════════════════════════════
# CARREGAR E FILTRAR DADOS
# ═══════════════════════════════════════════════════════════════════════════

try:
    df_raw = load_data()
    df = create_sidebar_filters(df_raw)
    df = get_success_only(df)
    
    # Filtros adicionais na sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🎚️ Filtros Avançados")
    
    # Range de ativo
    asset_range = create_asset_range_filter(df, key="market_asset")
    df = apply_asset_filter(df, asset_range[0], asset_range[1])
    
    # Range de NPL
    npl_range = create_npl_range_filter(df, key="market_npl")
    df = apply_npl_filter(df, npl_range[0], npl_range[1])
    
    st.sidebar.caption(f"📊 {len(df):,} registros após filtros")
    
    if len(df) == 0:
        st.warning("⚠️ Nenhum dado disponível com os filtros selecionados")
        st.stop()
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # KPIs PRINCIPAIS
    # ═══════════════════════════════════════════════════════════════════════
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        volume_total = df['ATIVO_TOTAL'].sum()
        st.metric(
            "💰 Volume Total",
            format_currency(volume_total),
            help="Soma do ativo total de todos os fundos"
        )
    
    with col2:
        total_fundos = df['CNPJ_FUNDO'].nunique()
        st.metric(
            "📁 Fundos Ativos",
            f"{total_fundos:,}"
        )
    
    with col3:
        npl_metrics = calculate_npl_metrics(df)
        npl_medio = npl_metrics.get('media', 0) * 100
        st.metric(
            "📊 NPL Médio",
            f"{npl_medio:.2f}%"
        )
    
    with col4:
        carteira_total = df['CARTEIRA_TOTAL'].sum() if 'CARTEIRA_TOTAL' in df.columns else 0
        st.metric(
            "📋 Carteira Total",
            format_currency(carteira_total)
        )
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # EVOLUÇÃO TEMPORAL
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 📈 Evolução Temporal")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Volume Total ao Longo do Tempo")
        df_evo_volume = calculate_temporal_evolution(df, 'ATIVO_TOTAL', agg_func='sum')
        
        if not df_evo_volume.empty:
            fig_volume = create_line_chart(
                df_evo_volume,
                x='periodo',
                y='valor',
                title=""
            )
            render_plotly_chart(fig_volume)
    
    with col2:
        st.markdown("#### NPL Médio ao Longo do Tempo")
        df_evo_npl = calculate_temporal_evolution(df, 'INDICE_NPL_DECIMAL', agg_func='mean')
        
        if not df_evo_npl.empty:
            df_evo_npl['valor'] = df_evo_npl['valor'] * 100  # Converter para %
            fig_npl = create_line_chart(
                df_evo_npl,
                x='periodo',
                y='valor',
                title=""
            )
            fig_npl.update_yaxes(title_text="NPL (%)")
            render_plotly_chart(fig_npl)
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # DISTRIBUIÇÃO POR SEGMENTO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 🏢 Distribuição por Segmento")
    
    df_segments = calculate_volume_by_segment(df)
    
    if not df_segments.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top 10 segmentos
            df_top = df_segments.head(10)
            fig_bar = create_bar_chart(
                df_top,
                x='volume',
                y='segmento',
                title="Top 10 Segmentos por Volume",
                orientation='h',
                text_auto=True
            )
            render_plotly_chart(fig_bar)
        
        with col2:
            # Pie chart
            fig_pie = create_pie_chart(
                df_segments.head(8),
                names='segmento',
                values='volume',
                title="Composição da Carteira"
            )
            render_plotly_chart(fig_pie)
    else:
        st.info("Dados de segmentação não disponíveis")
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # DISTRIBUIÇÃO DE NPL
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 📊 Distribuição de NPL")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'INDICE_NPL_DECIMAL' in df.columns:
            # Criar coluna de NPL em %
            df_hist = df.copy()
            df_hist['NPL_PCT'] = df_hist['INDICE_NPL_DECIMAL'] * 100
            
            fig_hist = create_histogram(
                df_hist,
                x='NPL_PCT',
                title="Distribuição de NPL (%)",
                nbins=30
            )
            fig_hist.update_xaxes(title_text="NPL (%)")
            fig_hist.update_yaxes(title_text="Quantidade de Fundos")
            render_plotly_chart(fig_hist)
    
    with col2:
        st.markdown("#### 📈 Estatísticas de NPL")
        
        if npl_metrics:
            col_a, col_b = st.columns(2)
            
            with col_a:
                st.metric("Média", format_percent(npl_metrics.get('media', 0), decimal=True))
                st.metric("Mínimo", format_percent(npl_metrics.get('min', 0), decimal=True))
                st.metric("Q1 (25%)", format_percent(npl_metrics.get('q25', 0), decimal=True))
            
            with col_b:
                st.metric("Mediana", format_percent(npl_metrics.get('mediana', 0), decimal=True))
                st.metric("Máximo", format_percent(npl_metrics.get('max', 0), decimal=True))
                st.metric("Q3 (75%)", format_percent(npl_metrics.get('q75', 0), decimal=True))
            
            # Alert para fundos em distress
            high_npl = df[df['INDICE_NPL_DECIMAL'] > 0.5]
            if len(high_npl) > 0:
                st.warning(f"⚠️ {len(high_npl)} fundos com NPL > 50%")

except FileNotFoundError:
    st.error("❌ Arquivo de dados não encontrado!")
    st.info("Execute o ETL primeiro: `python scripts/run_etl.py`")
    
except Exception as e:
    st.error(f"❌ Erro: {str(e)}")
    st.exception(e)

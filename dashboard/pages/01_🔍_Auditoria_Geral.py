"""
🔍 Auditoria Geral - Visão de Qualidade dos Dados

Esta página apresenta métricas de qualidade, completude e validação dos dados.
"""
import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import (
    load_data,
    get_last_update_info,
    calculate_completeness,
    calculate_flag_summary,
    calculate_temporal_evolution,
    format_currency,
    create_completeness_chart,
    create_bar_chart,
    render_plotly_chart,
    display_dataframe_styled,
    create_sidebar_filters
)
from config.settings import FLAG_COLUMNS

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Auditoria Geral | FIDC Monitor",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Auditoria Geral")
st.markdown("### Visão de Qualidade dos Dados")

# ═══════════════════════════════════════════════════════════════════════════
# CARREGAR E FILTRAR DADOS
# ═══════════════════════════════════════════════════════════════════════════

try:
    df_raw = load_data()
    df = create_sidebar_filters(df_raw)
    
    info = get_last_update_info(df)
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # KPIs DE QUALIDADE
    # ═══════════════════════════════════════════════════════════════════════
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "📁 Total Fundos",
            f"{info['total_fundos']:,}",
            help="Fundos únicos no período selecionado"
        )
    
    with col2:
        volume = df['ATIVO_TOTAL'].sum() if 'ATIVO_TOTAL' in df.columns else 0
        st.metric(
            "💰 Volume Total",
            format_currency(volume)
        )
    
    with col3:
        taxa_sucesso = (info['total_sucesso'] / info['total_registros'] * 100) if info['total_registros'] > 0 else 0
        st.metric(
            "✅ Taxa Sucesso",
            f"{taxa_sucesso:.1f}%"
        )
    
    with col4:
        st.metric(
            "📅 Última Competência",
            info['ultima_competencia'] or "N/A"
        )
    
    with col5:
        # Calcular completude geral
        total_cells = df.shape[0] * df.shape[1]
        non_null = df.notna().sum().sum()
        completude = (non_null / total_cells * 100) if total_cells > 0 else 0
        st.metric(
            "📊 Completude Geral",
            f"{completude:.1f}%"
        )
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # GRÁFICOS
    # ═══════════════════════════════════════════════════════════════════════
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### 📊 Completude por Coluna")
        
        df_completeness = calculate_completeness(df)
        fig_completeness = create_completeness_chart(df_completeness, top_n=20)
        render_plotly_chart(fig_completeness)
    
    with col2:
        st.markdown("#### 🚩 Flags de Qualidade")
        
        # Identificar colunas de flag existentes
        existing_flags = [col for col in FLAG_COLUMNS if col in df.columns]
        
        if existing_flags:
            df_flags = calculate_flag_summary(df, existing_flags)
            
            if not df_flags.empty:
                # Mostrar como tabela formatada
                for _, row in df_flags.iterrows():
                    emoji = "🔴" if row['quantidade'] > 0 else "🟢"
                    st.write(f"{emoji} **{row['flag']}**: {row['quantidade']} ({row['percentual']:.1f}%)")
            else:
                st.success("✅ Nenhuma flag ativa")
        else:
            st.info("ℹ️ Nenhuma coluna de flag encontrada")
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # DISTRIBUIÇÃO TEMPORAL
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("#### 📅 Distribuição Temporal de Fundos")
    
    if 'DATA_COMPETENCIA' in df.columns and 'CNPJ_FUNDO' in df.columns:
        df_temporal = calculate_temporal_evolution(df, 'CNPJ_FUNDO', agg_func='nunique')
        
        if not df_temporal.empty:
            fig_temporal = create_bar_chart(
                df_temporal,
                x='periodo',
                y='valor',
                title="Fundos por Período",
                text_auto=True
            )
            render_plotly_chart(fig_temporal)
        else:
            st.info("Sem dados temporais disponíveis")
    else:
        st.info("Coluna de data não encontrada")
    
    # ═══════════════════════════════════════════════════════════════════════
    # TABELA DE PROBLEMAS
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("---")
    st.markdown("#### ⚠️ Registros com Problemas")
    
    # Filtrar registros com alguma flag ativa
    existing_flags = [col for col in FLAG_COLUMNS if col in df.columns]
    
    if existing_flags:
        # Criar máscara de problemas
        mask = df[existing_flags].any(axis=1)
        df_problems = df[mask]
        
        if not df_problems.empty:
            st.warning(f"⚠️ {len(df_problems)} registros com problemas identificados")
            
            # Mostrar colunas relevantes
            cols_to_show = ['CNPJ_FUNDO', 'DATA_COMPETENCIA', 'ATIVO_TOTAL', 'STATUS'] + existing_flags
            cols_to_show = [c for c in cols_to_show if c in df_problems.columns]
            
            display_dataframe_styled(df_problems[cols_to_show].head(50))
            
            # Botão para download
            csv = df_problems.to_csv(index=False, sep=';', decimal=',')
            st.download_button(
                "📥 Baixar lista de problemas (CSV)",
                csv,
                "problemas_fidc.csv",
                "text/csv"
            )
        else:
            st.success("✅ Nenhum registro com problemas nas flags!")
    else:
        st.info("Nenhuma coluna de flag disponível para análise")

except FileNotFoundError:
    st.error("❌ Arquivo de dados não encontrado!")
    st.info("Execute o ETL primeiro: `python scripts/run_etl.py`")
    
except Exception as e:
    st.error(f"❌ Erro: {str(e)}")
    st.exception(e)

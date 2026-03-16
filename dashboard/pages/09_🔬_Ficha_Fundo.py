"""
🔬 Ficha do Fundo - Análise Individual

Esta página permite análise detalhada de um fundo específico.
"""
import streamlit as st
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import (
    load_data,
    get_success_only,
    get_unique_cnpjs,
    calculate_fund_metrics,
    format_currency,
    format_percent,
    create_line_chart,
    render_plotly_chart,
    display_dataframe_styled,
    create_fund_search
)
from config.settings import FLAG_COLUMNS

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Ficha do Fundo | FIDC Monitor",
    page_icon="🔬",
    layout="wide"
)

st.title("🔬 Ficha do Fundo")
st.markdown("### Análise Individual Detalhada")

# ═══════════════════════════════════════════════════════════════════════════
# CARREGAR DADOS
# ═══════════════════════════════════════════════════════════════════════════

try:
    df = load_data()
    df_success = get_success_only(df)
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # BUSCA DE FUNDO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 🔍 Selecione um Fundo")
    
    selected_cnpj = create_fund_search(df_success, key="fund_search_main")
    
    if not selected_cnpj:
        st.info("👆 Selecione um CNPJ acima para ver a análise detalhada")
        
        # Mostrar preview de fundos disponíveis
        st.markdown("---")
        st.markdown("#### 📋 Fundos Disponíveis")
        
        # Resumo por fundo
        if 'CNPJ_FUNDO' in df_success.columns:
            df_summary = df_success.groupby('CNPJ_FUNDO').agg({
                'ATIVO_TOTAL': 'last',
                'INDICE_NPL_DECIMAL': 'last',
                'DATA_COMPETENCIA': 'last'
            }).reset_index()
            
            df_summary.columns = ['CNPJ', 'Ativo Total', 'NPL', 'Última Competência']
            df_summary = df_summary.sort_values('Ativo Total', ascending=False).head(20)
            
            display_dataframe_styled(df_summary)
        
        st.stop()
    
    # ═══════════════════════════════════════════════════════════════════════
    # DADOS DO FUNDO SELECIONADO
    # ═══════════════════════════════════════════════════════════════════════
    
    # Garantir que CNPJ é string para comparação correta
    df_success['CNPJ_FUNDO'] = df_success['CNPJ_FUNDO'].astype(str)
    
    df_fund = df_success[df_success['CNPJ_FUNDO'] == str(selected_cnpj)].sort_values('DATA_COMPETENCIA')
    
    if df_fund.empty:
        st.error(f"❌ Nenhum dado encontrado para o CNPJ: {selected_cnpj}")
        st.stop()
    
    # Último registro
    latest = df_fund.iloc[-1]
    metrics = calculate_fund_metrics(df_success, str(selected_cnpj))
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # HEADER COM INFO BÁSICA
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown(f"## 📊 Fundo: `{selected_cnpj}`")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "💰 Ativo Total",
            format_currency(metrics.get('ativo_total', 0))
        )
    
    with col2:
        st.metric(
            "📊 NPL",
            format_percent(metrics.get('npl', 0))
        )
    
    with col3:
        st.metric(
            "💧 Liquidez",
            format_percent(metrics.get('liquidez', 0))
        )
    
    with col4:
        st.metric(
            "📅 Períodos",
            f"{len(df_fund)}"
        )
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # GRÁFICOS DE EVOLUÇÃO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 📈 Evolução Histórica")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Ativo Total
        if 'ATIVO_TOTAL' in df_fund.columns and 'DATA_COMPETENCIA' in df_fund.columns:
            fig_ativo = create_line_chart(
                df_fund,
                x='DATA_COMPETENCIA',
                y='ATIVO_TOTAL',
                title="💰 Ativo Total"
            )
            render_plotly_chart(fig_ativo)
    
    with col2:
        # NPL
        if 'INDICE_NPL_DECIMAL' in df_fund.columns:
            df_npl = df_fund.copy()
            df_npl['NPL_PCT'] = df_npl['INDICE_NPL_DECIMAL'] * 100
            
            fig_npl = create_line_chart(
                df_npl,
                x='DATA_COMPETENCIA',
                y='NPL_PCT',
                title="📊 NPL (%)"
            )
            render_plotly_chart(fig_npl)
    
    col3, col4 = st.columns(2)
    
    with col3:
        # Carteira
        if 'CARTEIRA_TOTAL' in df_fund.columns:
            fig_cart = create_line_chart(
                df_fund,
                x='DATA_COMPETENCIA',
                y='CARTEIRA_TOTAL',
                title="📋 Carteira Total"
            )
            render_plotly_chart(fig_cart)
    
    with col4:
        # Inadimplência
        if 'INADIMPLENCIA_TOTAL' in df_fund.columns:
            fig_inad = create_line_chart(
                df_fund,
                x='DATA_COMPETENCIA',
                y='INADIMPLENCIA_TOTAL',
                title="⚠️ Inadimplência Total"
            )
            render_plotly_chart(fig_inad)
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # FLAGS E ALERTAS
    # ═══════════════════════════════════════════════════════════════════════
    
    with st.expander("🚩 Flags e Alertas", expanded=False):
        existing_flags = [col for col in FLAG_COLUMNS if col in df_fund.columns]
        
        if existing_flags:
            has_issues = False
            
            for col in existing_flags:
                try:
                    if latest[col] == True or latest[col] == 'True':
                        st.warning(f"🔴 {col.replace('_FLAG', '').replace('_', ' ').title()}")
                        has_issues = True
                except:
                    pass
            
            if not has_issues:
                st.success("✅ Nenhuma flag ativa no último período")
        else:
            st.info("Nenhuma coluna de flag disponível")
    
    # ═══════════════════════════════════════════════════════════════════════
    # HISTÓRICO COMPLETO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("---")
    st.markdown("### 📋 Histórico Completo")
    
    # Selecionar colunas principais
    cols_to_show = [
        'DATA_COMPETENCIA', 'ATIVO_TOTAL', 'CARTEIRA_TOTAL',
        'INADIMPLENCIA_TOTAL', 'INDICE_NPL_DECIMAL', 'TAXA_LIQUIDEZ_DECIMAL',
        'STATUS'
    ]
    cols_to_show = [c for c in cols_to_show if c in df_fund.columns]
    
    display_dataframe_styled(df_fund[cols_to_show])
    
    # ═══════════════════════════════════════════════════════════════════════
    # EXPORTAÇÃO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("---")
    st.markdown("### 📥 Exportar Dados")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # CSV
        csv = df_fund.to_csv(index=False, sep=';', decimal=',')
        st.download_button(
            "📥 Baixar CSV Completo",
            csv,
            f"fundo_{selected_cnpj.replace('/', '_').replace('.', '_')}.csv",
            "text/csv"
        )
    
    with col2:
        # Resumo em texto
        resumo = f"""
FICHA DO FUNDO - {selected_cnpj}
{'='*50}

CNPJ: {selected_cnpj}
Última Competência: {latest.get('DATA_COMPETENCIA', 'N/A')}
Status: {latest.get('STATUS', 'N/A')}

MÉTRICAS ATUAIS:
- Ativo Total: {format_currency(metrics.get('ativo_total', 0))}
- Carteira Total: {format_currency(metrics.get('carteira_total', 0))}
- Inadimplência: {format_currency(metrics.get('inadimplencia', 0))}
- NPL: {format_percent(metrics.get('npl', 0))}
- Liquidez: {format_percent(metrics.get('liquidez', 0))}

Total de Períodos: {len(df_fund)}
"""
        st.download_button(
            "📄 Baixar Resumo (TXT)",
            resumo,
            f"resumo_{selected_cnpj.replace('/', '_').replace('.', '_')}.txt",
            "text/plain"
        )

except FileNotFoundError:
    st.error("❌ Arquivo de dados não encontrado!")
    st.info("Execute o ETL primeiro: `python scripts/run_etl.py`")
    
except Exception as e:
    st.error(f"❌ Erro: {str(e)}")
    st.exception(e)

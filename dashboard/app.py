"""
📊 FIDC Monitor - Dashboard de Análise de FIDCs

Dashboard interativo para auditoria e análise de Fundos de Investimento
em Direitos Creditórios (FIDCs) brasileiros.

Autor: Rafael Augusto
Data: Janeiro 2026
"""
import streamlit as st
import sys
from pathlib import Path

# Adicionar diretório ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    load_data,
    validate_dataframe,
    get_last_update_info,
    format_currency,
    display_kpi_row
)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DA PÁGINA
# ═══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="FIDC Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("📊 FIDC Monitor")
    st.markdown("Dashboard de Análise de FIDCs")
    st.markdown("---")
    
    # Tentar carregar dados para mostrar info
    try:
        df = load_data()
        info = get_last_update_info(df)
        
        st.caption(f"📅 Última competência: {info['ultima_competencia']}")
        st.caption(f"📁 Total de fundos: {info['total_fundos']:,}")
        st.caption(f"✅ Registros sucesso: {info['total_sucesso']:,}")
        
        # Calcular volume total
        if 'ATIVO_TOTAL' in df.columns:
            volume = df[df['STATUS'] == 'SUCESSO']['ATIVO_TOTAL'].sum()
            st.caption(f"💰 Volume: {format_currency(volume)}")
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
    
    st.markdown("---")
    st.markdown("### 📚 Navegação")
    st.markdown("""
    - 🔍 **Auditoria Geral** - Qualidade dos dados
    - 🌍 **Panorama do Mercado** - Visão macro
    - 🔬 **Ficha do Fundo** - Análise individual
    """)

# ═══════════════════════════════════════════════════════════════════════════
# PÁGINA PRINCIPAL - HOME
# ═══════════════════════════════════════════════════════════════════════════

st.title("📊 FIDC Monitor")
st.markdown("### Dashboard de Auditoria e Análise de FIDCs")

st.markdown("---")

# Carregar dados
try:
    df = load_data()
    is_valid, errors = validate_dataframe(df)
    
    if not is_valid:
        st.error("❌ Problemas na estrutura dos dados:")
        for error in errors:
            st.warning(f"⚠️ {error}")
        st.stop()
    
    # Filtrar apenas sucesso para métricas principais
    df_success = df[df['STATUS'] == 'SUCESSO'] if 'STATUS' in df.columns else df
    
    # ═══════════════════════════════════════════════════════════════════════
    # KPIs RESUMO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 📈 Resumo Executivo")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_fundos = len(df_success['CNPJ_FUNDO'].unique())
        st.metric("📁 Fundos", f"{total_fundos:,}")
    
    with col2:
        volume_total = df_success['ATIVO_TOTAL'].sum()
        st.metric("💰 Volume Total", format_currency(volume_total))
    
    with col3:
        if 'STATUS' in df.columns:
            taxa_sucesso = (df['STATUS'] == 'SUCESSO').sum() / len(df) * 100
            st.metric("✅ Taxa Sucesso", f"{taxa_sucesso:.1f}%")
        else:
            st.metric("✅ Taxa Sucesso", "N/A")
    
    with col4:
        npl_medio = df_success['INDICE_NPL_DECIMAL'].mean() * 100 if 'INDICE_NPL_DECIMAL' in df_success.columns else 0
        st.metric("📊 NPL Médio", f"{npl_medio:.2f}%")
    
    with col5:
        periodos = df_success['DATA_COMPETENCIA'].nunique() if 'DATA_COMPETENCIA' in df_success.columns else 0
        st.metric("📅 Períodos", f"{periodos}")
    
    st.markdown("---")
    
    # ═══════════════════════════════════════════════════════════════════════
    # CARDS DE NAVEGAÇÃO
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("### 🧭 Navegue pelo Dashboard")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        #### 🔍 Auditoria de Dados
        
        Verifique a qualidade, consistência e completude dos dados extraídos.
        
        - Completude por coluna
        - Flags de problemas
        - Distribuição temporal
        """)
        if st.button("Ir para Auditoria →", key="btn_audit"):
            st.switch_page("pages/01_🔍_Auditoria_Geral.py")
    
    with col2:
        st.markdown("""
        #### 🌍 Panorama do Mercado
        
        Visão consolidada do mercado de FIDCs brasileiro.
        
        - Volume por segmento
        - Evolução temporal
        - Distribuição de NPL
        """)
        if st.button("Ir para Panorama →", key="btn_market"):
            st.switch_page("pages/05_🌍_Panorama_Mercado.py")
    
    with col3:
        st.markdown("""
        #### 🔬 Análise Individual
        
        Drill-down para análise detalhada de cada fundo.
        
        - Ficha completa do fundo
        - Evolução histórica
        - Comparação com mercado
        """)
        if st.button("Ir para Ficha →", key="btn_fund"):
            st.switch_page("pages/09_🔬_Ficha_Fundo.py")
    
    # ═══════════════════════════════════════════════════════════════════════
    # DESTAQUES
    # ═══════════════════════════════════════════════════════════════════════
    
    st.markdown("---")
    st.markdown("### 🔔 Destaques")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📊 Distribuição por Status")
        if 'STATUS' in df.columns:
            status_counts = df['STATUS'].value_counts()
            for status, count in status_counts.items():
                pct = count / len(df) * 100
                emoji = "✅" if status == "SUCESSO" else "❌"
                st.write(f"{emoji} **{status}**: {count:,} ({pct:.1f}%)")
    
    with col2:
        st.markdown("#### ⚠️ Alertas")
        
        # Verificar flags
        flag_cols = [col for col in df_success.columns if col.endswith('_FLAG')]
        alerts = []
        
        for col in flag_cols:
            try:
                count = df_success[col].astype(bool).sum()
                if count > 0:
                    name = col.replace('_FLAG', '').replace('_', ' ').title()
                    alerts.append(f"🚩 **{name}**: {count} fundos")
            except:
                pass
        
        if alerts:
            for alert in alerts[:5]:  # Limitar a 5
                st.write(alert)
        else:
            st.success("✅ Nenhum alerta identificado")

except FileNotFoundError:
    st.error("❌ Arquivo de dados não encontrado!")
    st.info("📁 Certifique-se de que o arquivo está em `outputs/cleaned_snapshot_latest.csv`")
    st.info("🔄 Execute o ETL primeiro: `python scripts/run_etl.py`")
    
except Exception as e:
    st.error(f"❌ Erro inesperado: {str(e)}")
    st.exception(e)

# ═══════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("---")
st.caption("📊 FIDC Monitor v1.0 | Desenvolvido por Rafael Augusto | Janeiro 2026")

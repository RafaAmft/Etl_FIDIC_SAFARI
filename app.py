"""
FIDC ETL Monitor — Interface Streamlit.

Ponto de entrada da aplicação Streamlit.
Lê CSVs exportados pelo ETL (que roda separado).

Uso:
    streamlit run app.py
"""
import streamlit as st

from src.backend import (
    get_distressed_npl,
    get_estatisticas_gerais,
    get_fundo_historico,
    get_fundos_resumo,
    get_safari_oportunidades,
    listar_exports_disponiveis,
)

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="FIDC ETL Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Sidebar — seleção de export
# ---------------------------------------------------------------------------
st.sidebar.title("FIDC ETL Monitor")
st.sidebar.markdown("---")

exports = listar_exports_disponiveis()
if exports:
    opcoes = [f"{e['data']} — {e['arquivo']}" for e in exports]
    st.sidebar.selectbox("Export disponível:", opcoes)
else:
    st.sidebar.warning("Nenhum export encontrado. Execute o ETL primeiro.")

st.sidebar.markdown("---")
st.sidebar.caption("ETL roda separado. Esta interface apenas lê os CSVs exportados.")

# ---------------------------------------------------------------------------
# Corpo principal
# ---------------------------------------------------------------------------
st.title("FIDC Safari Monitor")
st.markdown("Monitor de fundos FIDC distressed para identificação de oportunidades.")

# --- Estatísticas gerais ---
stats = get_estatisticas_gerais()
if "error" not in stats:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Fundos", stats.get("total_fundos", 0))
    col2.metric("NPL Médio (%)", f"{stats.get('media_npl', 0.0):.2f}")
    col3.metric("Score Médio", f"{stats.get('media_score', 0.0):.2f}")
    col4.metric("Alta Oportunidade", stats.get("fundos_alta_oportunidade", 0))
else:
    st.warning(f"Dados não disponíveis: {stats['error']}")

st.markdown("---")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Safari Oportunidades", "Fundos Distressed", "Todos os Fundos"])

with tab1:
    st.subheader("Oportunidades Safari (Score >= 30)")
    score_min = st.slider("Score mínimo:", 0.0, 100.0, 30.0, 5.0)
    df_safari = get_safari_oportunidades(score_min=score_min)
    if not df_safari.empty:
        st.dataframe(df_safari, use_container_width=True)
        st.caption(f"{len(df_safari)} fundos encontrados.")
    else:
        st.info("Nenhuma oportunidade encontrada com os filtros selecionados.")

with tab2:
    st.subheader("Fundos Distressed por NPL")
    npl_min = st.slider("NPL mínimo (%):", 0.0, 200.0, 20.0, 5.0)
    df_npl = get_distressed_npl(npl_min=npl_min)
    if not df_npl.empty:
        st.dataframe(df_npl, use_container_width=True)
        st.caption(f"{len(df_npl)} fundos com NPL >= {npl_min:.0f}%.")
    else:
        st.info("Nenhum fundo distressed encontrado.")

with tab3:
    st.subheader("Todos os Fundos")
    filtro = st.selectbox(
        "Filtrar por classificação:",
        ["(todos)", "ALTA_OPORTUNIDADE", "OPORTUNIDADE_MODERADA", "MONITORAR",
         "BAIXA_PRIORIDADE", "SEM_OPORTUNIDADE", "DADOS_INVALIDOS"],
    )
    df_all = get_fundos_resumo(filtro_classificacao=filtro if filtro != "(todos)" else None)
    if not df_all.empty:
        st.dataframe(df_all, use_container_width=True)
        st.caption(f"{len(df_all)} registros.")
    else:
        st.info("Nenhum dado disponível.")

# --- Histórico de CNPJ ---
st.markdown("---")
st.subheader("Histórico por CNPJ")
cnpj_input = st.text_input("CNPJ (14 dígitos ou formatado):", placeholder="12.345.678/0001-95")
if cnpj_input:
    df_hist = get_fundo_historico(cnpj_input)
    if not df_hist.empty:
        st.line_chart(df_hist.set_index("DATA_COMPETENCIA")[["NPL_RATIO", "SCORE_SAFARI"]])
        st.dataframe(df_hist, use_container_width=True)
    else:
        st.warning(f"Nenhum dado encontrado para CNPJ: {cnpj_input}")

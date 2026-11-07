"""
Backend público do FIDC ETL para consumo pelo Streamlit.

Todas as funções são:
- Stateless (sem variáveis globais)
- Type-annotated (inputs e outputs completos)
- Tolerantes a falhas (retornam DataFrame vazio ou dict com 'error', nunca None)
- Seguras para leitura concorrente (apenas I/O de leitura de CSVs)

Uso:
    from src.backend import get_fundos_resumo, get_safari_oportunidades
    df = get_fundos_resumo()
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config.settings import settings

logger = logging.getLogger(__name__)

# Colunas garantidas no DataFrame de saída
COLUNAS_RESUMO = [
    "CNPJ_FUNDO",
    "DATA_COMPETENCIA",
    "ATIVO_TOTAL",
    "PATRIMONIO_LIQUIDO",
    "NPL_RATIO",
    "ZUMBI_RATIO",
    "LIQUIDEZ_IMEDIATA_RATIO",
    "SCORE_SAFARI",
    "CLASSIFICACAO_SAFARI",
    "STATUS_DADOS",
    "ANOMALIA_ESTRUTURAL",
]

COLUNAS_SAFARI = [
    "CNPJ_FUNDO",
    "DATA_COMPETENCIA",
    "SCORE_SAFARI",
    "CLASSIFICACAO_SAFARI",
    "NPL_RATIO",
    "ZUMBI_RATIO",
    "ATIVO_TOTAL",
    "PATRIMONIO_LIQUIDO",
]

COLUNAS_HISTORICO = [
    "DATA_COMPETENCIA",
    "NPL_RATIO",
    "ZUMBI_RATIO",
    "LIQUIDEZ_IMEDIATA_RATIO",
    "SCORE_SAFARI",
    "ATIVO_TOTAL",
    "PATRIMONIO_LIQUIDO",
    "CLASSIFICACAO_SAFARI",
]


def _find_latest_csv(pattern: str) -> Optional[Path]:
    """
    Encontra o CSV mais recente em data/output/**/ que corresponde ao padrão.

    Args:
        pattern: Padrão glob do nome do arquivo (ex: 'fidc_monitor_completo_*.csv').

    Returns:
        Path do arquivo mais recente, ou None se não encontrado.
    """
    output_dir = settings.output_dir
    if not output_dir.exists():
        return None

    matches = sorted(output_dir.glob(f"**/{pattern}"), key=lambda p: p.stat().st_mtime)
    return matches[-1] if matches else None


def _load_csv(filepath: Path) -> pd.DataFrame:
    """
    Lê CSV com encoding utf-8-sig (compatível com BOM do Excel).

    Args:
        filepath: Caminho do arquivo.

    Returns:
        DataFrame com os dados, ou DataFrame vazio em caso de erro.
    """
    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig", low_memory=False)
        logger.debug(f"CSV carregado: {filepath} ({len(df)} linhas)")
        return df
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        logger.error(f"Erro ao ler CSV {filepath}: {exc}")
        return pd.DataFrame()


def _ensure_columns(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """Garante que as colunas esperadas existam no DataFrame (preenche com NaN se ausentes)."""
    for col in colunas:
        if col not in df.columns:
            df[col] = None
    return df


def get_fundos_resumo(filtro_classificacao: Optional[str] = None) -> pd.DataFrame:
    """
    Retorna DataFrame com resumo dos fundos FIDC do último export.

    Colunas garantidas:
        CNPJ_FUNDO, DATA_COMPETENCIA, ATIVO_TOTAL, PATRIMONIO_LIQUIDO,
        NPL_RATIO, ZUMBI_RATIO, LIQUIDEZ_IMEDIATA_RATIO, SCORE_SAFARI,
        CLASSIFICACAO_SAFARI, STATUS_DADOS, ANOMALIA_ESTRUTURAL

    Args:
        filtro_classificacao: Se informado, filtra por CLASSIFICACAO_SAFARI
                              (ex: 'ALTA_OPORTUNIDADE').

    Returns:
        DataFrame com resumo dos fundos. Retorna DataFrame vazio em caso de erro.
    """
    csv_path = _find_latest_csv("fidc_monitor_completo_*.csv")
    if csv_path is None:
        logger.warning("Nenhum CSV de monitor completo encontrado em data/output/")
        return pd.DataFrame(columns=COLUNAS_RESUMO)

    df = _load_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=COLUNAS_RESUMO)

    df = _ensure_columns(df, COLUNAS_RESUMO)

    if filtro_classificacao and "CLASSIFICACAO_SAFARI" in df.columns:
        df = df[df["CLASSIFICACAO_SAFARI"] == filtro_classificacao]

    return df[COLUNAS_RESUMO].copy()


def get_safari_oportunidades(score_min: float = 30.0) -> pd.DataFrame:
    """
    Retorna fundos com SCORE_SAFARI >= score_min do último export.

    Colunas garantidas:
        CNPJ_FUNDO, DATA_COMPETENCIA, SCORE_SAFARI, CLASSIFICACAO_SAFARI,
        NPL_RATIO, ZUMBI_RATIO, ATIVO_TOTAL, PATRIMONIO_LIQUIDO

    Args:
        score_min: Score mínimo para inclusão (default: 30.0).

    Returns:
        DataFrame ordenado por SCORE_SAFARI descendente.
        Retorna DataFrame vazio em caso de erro ou sem dados.
    """
    # Tentar CSV de oportunidades primeiro; fallback para monitor completo
    csv_path = _find_latest_csv("fidc_safari_oportunidades_*.csv")
    if csv_path is None:
        csv_path = _find_latest_csv("fidc_monitor_completo_*.csv")

    if csv_path is None:
        logger.warning("Nenhum CSV de Safari encontrado em data/output/")
        return pd.DataFrame(columns=COLUNAS_SAFARI)

    df = _load_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=COLUNAS_SAFARI)

    df = _ensure_columns(df, COLUNAS_SAFARI)

    if "SCORE_SAFARI" in df.columns:
        df["SCORE_SAFARI"] = pd.to_numeric(df["SCORE_SAFARI"], errors="coerce").fillna(0.0)
        df = df[df["SCORE_SAFARI"] >= score_min]

    df = df.sort_values("SCORE_SAFARI", ascending=False, ignore_index=True)
    return df[COLUNAS_SAFARI].copy()


def get_distressed_npl(npl_min: float = 20.0) -> pd.DataFrame:
    """
    Retorna fundos com NPL_RATIO >= npl_min, ordenados por NPL descendente.

    Args:
        npl_min: NPL mínimo em % (default: 20.0).

    Returns:
        DataFrame com fundos distressed. Retorna DataFrame vazio em caso de erro.
    """
    colunas = ["CNPJ_FUNDO", "DATA_COMPETENCIA", "NPL_RATIO", "ZUMBI_RATIO",
               "ATIVO_TOTAL", "STATUS_DADOS", "SCORE_SAFARI"]

    csv_path = _find_latest_csv("fidc_distressed_npl_*.csv")
    if csv_path is None:
        csv_path = _find_latest_csv("fidc_monitor_completo_*.csv")

    if csv_path is None:
        logger.warning("Nenhum CSV distressed/monitor encontrado em data/output/")
        return pd.DataFrame(columns=colunas)

    df = _load_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=colunas)

    df = _ensure_columns(df, colunas)

    if "NPL_RATIO" in df.columns:
        df["NPL_RATIO"] = pd.to_numeric(df["NPL_RATIO"], errors="coerce").fillna(0.0)
        df = df[df["NPL_RATIO"] >= npl_min]

    df = df.sort_values("NPL_RATIO", ascending=False, ignore_index=True)
    return df[colunas].copy()


def get_fundo_historico(cnpj: str) -> pd.DataFrame:
    """
    Retorna série temporal de métricas para um CNPJ específico.

    Colunas garantidas:
        DATA_COMPETENCIA, NPL_RATIO, ZUMBI_RATIO, LIQUIDEZ_IMEDIATA_RATIO,
        SCORE_SAFARI, ATIVO_TOTAL, PATRIMONIO_LIQUIDO, CLASSIFICACAO_SAFARI

    Args:
        cnpj: CNPJ do fundo (14 dígitos, com ou sem formatação).

    Returns:
        DataFrame com histórico do fundo, ordenado por DATA_COMPETENCIA.
        Retorna DataFrame vazio se o CNPJ não for encontrado.
    """
    import re
    cnpj_limpo = re.sub(r"\D", "", str(cnpj))

    csv_path = _find_latest_csv("fidc_monitor_completo_*.csv")
    if csv_path is None:
        return pd.DataFrame(columns=COLUNAS_HISTORICO)

    df = _load_csv(csv_path)
    if df.empty or "CNPJ_FUNDO" not in df.columns:
        return pd.DataFrame(columns=COLUNAS_HISTORICO)

    df = _ensure_columns(df, COLUNAS_HISTORICO)

    df_fundo = df[df["CNPJ_FUNDO"].astype(str).str.replace(r"\D", "", regex=True) == cnpj_limpo]

    if df_fundo.empty:
        logger.info(f"Nenhum dado encontrado para CNPJ {cnpj_limpo}")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)

    df_fundo = df_fundo.sort_values("DATA_COMPETENCIA", ascending=True, ignore_index=True)
    return df_fundo[COLUNAS_HISTORICO].copy()


def get_estatisticas_gerais() -> Dict[str, Any]:
    """
    Retorna estatísticas agregadas de todos os fundos do último export.

    Returns:
        dict com chaves:
            total_fundos (int), media_npl (float), media_score (float),
            total_ativo (float), ultima_atualizacao (str),
            fundos_alta_oportunidade (int), fundos_com_anomalia (int)

        Em caso de erro, retorna dict com chave 'error'.
    """
    csv_path = _find_latest_csv("fidc_monitor_completo_*.csv")
    if csv_path is None:
        return {"error": "Nenhum export disponível. Execute o ETL primeiro."}

    df = _load_csv(csv_path)
    if df.empty:
        return {"error": "CSV encontrado mas está vazio ou corrompido."}

    try:
        npl_col = pd.to_numeric(df.get("NPL_RATIO", pd.Series(dtype=float)), errors="coerce")
        score_col = pd.to_numeric(df.get("SCORE_SAFARI", pd.Series(dtype=float)), errors="coerce")
        ativo_col = pd.to_numeric(df.get("ATIVO_TOTAL", pd.Series(dtype=float)), errors="coerce")

        fundos_alta = 0
        if "CLASSIFICACAO_SAFARI" in df.columns:
            fundos_alta = int((df["CLASSIFICACAO_SAFARI"] == "ALTA_OPORTUNIDADE").sum())

        fundos_anomalia = 0
        if "ANOMALIA_ESTRUTURAL" in df.columns:
            fundos_anomalia = int(df["ANOMALIA_ESTRUTURAL"].astype(str).isin(["True", "1", "true"]).sum())

        cnpjs_unicos = df["CNPJ_FUNDO"].nunique() if "CNPJ_FUNDO" in df.columns else 0

        return {
            "total_registros": len(df),
            "total_fundos": cnpjs_unicos,
            "media_npl": round(float(npl_col.mean(skipna=True)), 2),
            "media_score": round(float(score_col.mean(skipna=True)), 2),
            "total_ativo": round(float(ativo_col.sum(skipna=True)), 2),
            "fundos_alta_oportunidade": fundos_alta,
            "fundos_com_anomalia": fundos_anomalia,
            "ultima_atualizacao": csv_path.stem.split("_", 3)[-1],
            "arquivo_fonte": csv_path.name,
        }
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Erro ao calcular estatísticas: {exc}")
        return {"error": str(exc)}


def listar_exports_disponiveis() -> List[Dict[str, Any]]:
    """
    Lista CSVs do tipo 'monitor_completo' disponíveis em data/output/.

    Returns:
        Lista de dicts com:
            data (str), arquivo (str), caminho (str), tamanho_mb (float)
        Ordenada por data descendente. Retorna lista vazia se não encontrar nenhum.
    """
    output_dir = settings.output_dir
    if not output_dir.exists():
        return []

    exports: List[Dict[str, Any]] = []
    for csv_file in sorted(
        output_dir.glob("**/fidc_monitor_completo_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ):
        try:
            size_mb = round(csv_file.stat().st_size / 1_048_576, 2)
            # Extrair data do path (ex: data/output/2025-01-15/fidc_...)
            data_str = csv_file.parent.name
            exports.append(
                {
                    "data": data_str,
                    "arquivo": csv_file.name,
                    "caminho": str(csv_file),
                    "tamanho_mb": size_mb,
                }
            )
        except OSError:
            continue

    return exports

"""
FIDC ETL V8 - Pipeline Principal (Versão Refatorada Completa)

Este é o ponto de entrada do ETL modular de Fundos FIDC.
Inclui: processamento paralelo, filtro de classe, rolling window.
"""
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dateutil.relativedelta import relativedelta

from config.settings import settings
from src.utils.logging import setup_logging
from src.api.b3_api import B3API
from src.persistence.cache import CacheManager
from src.extraction.field_extractor import FieldExtractor
from src.transformation.metrics_calculator import MetricsCalculator
from src.transformation.anomaly_detector import AnomalyDetector
from src.transformation.safari_scorer import SafariScorer
from src.persistence.csv_exporter import CSVExporter

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# CLASSIFICADOR DE CLASSE
# ═══════════════════════════════════════════════════════════════════════

def classificar_prioridade_classe(nome_fundo: str) -> int:
    """
    Classifica a prioridade da classe de cota ANTES do download.

    Hierarquia:
    1 = Senior/Sênior (menor risco — preferível)
    2 = Mezanino (risco médio)
    3 = Subordinada/Júnior (maior risco)
    4 = Outros (não identificado)
    """
    if pd.isna(nome_fundo):
        return 4

    nome = str(nome_fundo).upper()

    if 'SENIOR' in nome or 'SÊNIOR' in nome:
        return 1
    elif any(x in nome for x in ['MEZANINO', 'MEZZANINE', 'MEZA']):
        return 2
    elif any(x in nome for x in ['SUBORDINADA', 'JÚNIOR', 'JUNIOR', 'SUB SENIOR', 'SUB-SENIOR']):
        return 3
    else:
        return 4


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE POR CNPJ (COM FILTRO DE CLASSE + ROLLING WINDOW)
# ═══════════════════════════════════════════════════════════════════════

def pipeline_por_cnpj(
    cnpj: str,
    nome_fundo_referencia: str,
    api: B3API,
    cache: CacheManager,
    extractor: FieldExtractor
) -> List[Dict[str, Any]]:
    """
    Executa pipeline completo para um CNPJ com:
    - Filtro rolling 60 meses
    - Seleção inteligente de classe (Senior > Mezanino > Subordinada)
    - Cache com versionamento
    
    Args:
        cnpj: CNPJ do fundo (14 dígitos)
        nome_fundo_referencia: Nome do fundo (para logging)
        api: Instância do cliente B3
        cache: Instância do gerenciador de cache
        extractor: Instância do extrator de campos
        
    Returns:
        Lista de dados extraídos
    """
    resultados = []
    classe_labels = {1: 'Senior', 2: 'Mezanino', 3: 'Subordinada', 4: 'Outros'}
    
    try:
        # ═══════════════════════════════════════════════════════════════
        # ETAPA 1: DISCOVERY
        # ═══════════════════════════════════════════════════════════════
        documentos = api.buscar_documentos(cnpj)
        if not documentos:
            logger.debug(f"CNPJ {cnpj}: Nenhum documento encontrado")
            return resultados

        df_docs = pd.DataFrame(documentos)

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 2: FILTRAR informes mensais ativos
        # ═══════════════════════════════════════════════════════════════
        df_mensal = df_docs[
            (df_docs['tipoDocumento'].str.strip() == "Informe Mensal Estruturado") &
            (df_docs['situacaoDocumento'].str.strip() == "A")
        ].copy()

        if df_mensal.empty:
            logger.debug(f"CNPJ {cnpj}: Nenhum informe mensal ativo")
            return resultados

        # Garantir coluna dataEntrega
        if 'dataEntrega' not in df_mensal.columns:
            df_mensal['dataEntrega'] = pd.NA

        # Converter datas
        df_mensal['dataReferenciaOrdenavel'] = pd.to_datetime(
            df_mensal['dataReferencia'].str[3:] + '-' +
            df_mensal['dataReferencia'].str[:2] + '-01',
            format='%Y-%m-%d', errors='coerce'
        )
        df_mensal['dataEntregaOrdenavel'] = pd.to_datetime(
            df_mensal['dataEntrega'],
            format='%d/%m/%Y %H:%M',
            errors='coerce'
        )

        df_mensal.dropna(
            subset=['dataReferenciaOrdenavel', 'dataEntregaOrdenavel'],
            inplace=True
        )
        if df_mensal.empty:
            logger.debug(f"CNPJ {cnpj}: Datas inválidas após conversão")
            return resultados

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 3: ROLLING WINDOW (últimos N meses)
        # ═══════════════════════════════════════════════════════════════
        data_limite = datetime.now() - relativedelta(
            months=settings.etl_rolling_window_months
        )
        df_mensal = df_mensal[
            df_mensal['dataReferenciaOrdenavel'] >= data_limite
        ].copy()

        if df_mensal.empty:
            logger.debug(
                f"CNPJ {cnpj}: Nenhum doc nos últimos "
                f"{settings.etl_rolling_window_months} meses"
            )
            return resultados

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 4: FILTRO INTELIGENTE DE CLASSE
        # Seleciona 1 doc por mês: Senior > Mezanino > Subordinada
        # ═══════════════════════════════════════════════════════════════
        df_mensal['prioridade_classe'] = df_mensal['descricaoFundo'].apply(
            classificar_prioridade_classe
        )
        df_mensal['ano_mes'] = df_mensal['dataReferenciaOrdenavel'].dt.to_period('M')

        df_mensal = df_mensal.sort_values(
            ['ano_mes', 'prioridade_classe', 'dataEntregaOrdenavel'],
            ascending=[True, True, False]
        )
        df_unique = df_mensal.groupby('ano_mes').first().reset_index()

        # Log
        classe_counts = df_unique['prioridade_classe'].value_counts()
        classes_info = ', '.join([
            f"{classe_labels.get(k, 'Outros')}: {v}"
            for k, v in classe_counts.items()
        ])
        logger.info(
            f"CNPJ {cnpj}: {len(df_unique)} meses únicos "
            f"(Classes: {classes_info})"
        )

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 5-7: DOWNLOAD → EXTRACT → TRANSFORM (COM CACHE)
        # ═══════════════════════════════════════════════════════════════
        for _, doc_row in df_unique.iterrows():
            doc_id = str(doc_row['id'])
            data_referencia = doc_row.get('dataReferencia', 'N/A')
            nome_fundo_doc = doc_row.get('descricaoFundo', 'Nome N/A')
            prioridade = doc_row.get('prioridade_classe', 4)

            # 5.1. Verificar cache
            dados_cached = cache.load(cnpj, doc_id)
            if dados_cached:
                colunas_essenciais = ['ANOMALIA_ESTRUTURAL', 'SCORE_SAFARI', 'STATUS_DADOS']
                if all(col in dados_cached for col in colunas_essenciais):
                    resultados.append(dados_cached)
                    continue
                else:
                    logger.debug(f"Cache antigo (sem colunas novas): {cnpj} | doc {doc_id}")

            # 5.2. Download XML
            xml_content = api.download_xml(doc_id)
            if not xml_content:
                resultados.append({
                    'CNPJ_FUNDO': cnpj,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_DOWNLOAD',
                    'ID_DOCUMENTO': doc_id,
                    'DATA_REFERENCIA_DOC': data_referencia,
                    'MENSAGEM_ERRO': 'Download retornou None',
                    'TIPO_COLETA': 'ERRO'
                })
                continue

            # 5.3. Extrair campos
            try:
                dados = extractor.extract_all_fields(xml_content)
            except Exception as e:
                logger.error(f"Parse error doc {doc_id}: {e}")
                resultados.append({
                    'CNPJ_FUNDO': cnpj,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_PARSE_XML',
                    'ID_DOCUMENTO': doc_id,
                    'DATA_REFERENCIA_DOC': data_referencia,
                    'MENSAGEM_ERRO': str(e),
                    'TIPO_COLETA': 'ERRO'
                })
                continue

            # 5.4. Calcular métricas
            MetricsCalculator.add_metrics_to_data(dados)

            # 5.5. Detectar anomalias
            AnomalyDetector.add_anomaly_flags(dados)

            # 5.6. Calcular Safari score
            SafariScorer.add_safari_score(dados)

            # 5.7. Enriquecer com metadados
            if not dados.get('CNPJ_FUNDO'):
                dados['CNPJ_FUNDO'] = cnpj
            dados['NOME_FUNDO'] = nome_fundo_doc
            dados['STATUS'] = 'SUCESSO'
            dados['ID_DOCUMENTO'] = doc_id
            dados['DATA_REFERENCIA_DOC'] = data_referencia
            dados['MENSAGEM_ERRO'] = None
            dados['TIPO_COLETA'] = 'ROLLING_60_MESES'
            dados['CLASSE_SELECIONADA'] = classe_labels.get(prioridade, 'Outros')
            dados['PRIORIDADE_CLASSE'] = prioridade

            # 5.8. Salvar cache
            cache.save(cnpj, doc_id, dados)
            resultados.append(dados)

            # Rate limiting
            time.sleep(settings.b3_delay_requisicoes)

    except Exception as e:
        logger.error(f"Erro geral CNPJ {cnpj}: {e}", exc_info=True)
        resultados.append({
            'CNPJ_FUNDO': cnpj,
            'NOME_FUNDO': nome_fundo_referencia,
            'STATUS': 'ERRO_GERAL',
            'MENSAGEM_ERRO': str(e),
            'TIPO_COLETA': 'ERRO'
        })

    return resultados


# ═══════════════════════════════════════════════════════════════════════
# CARGA DE CNPJs
# ═══════════════════════════════════════════════════════════════════════

def carregar_cnpjs(input_file: Path) -> pd.DataFrame:
    """
    Carrega e valida lista de CNPJs do arquivo CSV.
    
    Returns:
        DataFrame com colunas ['CNPJ', 'NOME_FUNDO']
    """
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    
    # Normalizar nome da coluna
    col_cnpj = None
    for col in df.columns:
        if col.upper() == 'CNPJ':
            col_cnpj = col
            break
    
    if col_cnpj is None:
        raise ValueError(
            f"Coluna 'CNPJ' não encontrada. Colunas disponíveis: {list(df.columns)}"
        )
    
    # Padronizar CNPJs: converter float→int→str, zfill 14 dígitos
    df['CNPJ'] = (
        df[col_cnpj]
        .apply(lambda x: str(int(float(x))) if pd.notna(x) else '')
        .str.zfill(14)
    )
    
    # Garantir coluna NOME_FUNDO
    if 'NOME_FUNDO' not in df.columns:
        df['NOME_FUNDO'] = ''
    
    # Remover duplicatas e inválidos
    df = df[df['CNPJ'].str.len() == 14].drop_duplicates(subset=['CNPJ'])
    
    logger.info(f"📋 {len(df)} CNPJs únicos carregados de {input_file.name}")
    return df[['CNPJ', 'NOME_FUNDO']]


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════

def main():
    """Pipeline principal do ETL V8 — com processamento paralelo"""
    
    # 1. Setup de logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = settings.logs_dir / f"etl_fidc_{timestamp}.log"
    setup_logging(log_file)
    
    logger.info("=" * 80)
    logger.info("FIDC ETL V8.0 - Monitor de Fundos Distressed")
    logger.info("=" * 80)
    
    try:
        # 2. Inicializar componentes
        api = B3API()
        cache = CacheManager()
        extractor = FieldExtractor()
        
        logger.info(f"⚙️ Configurações:")
        logger.info(f"   • Rolling window: {settings.etl_rolling_window_months} meses")
        logger.info(f"   • Max workers: {settings.etl_max_workers}")
        logger.info(f"   • Delay entre requisições: {settings.b3_delay_requisicoes}s")
        logger.info(f"   • Cache: {'Habilitado' if settings.etl_cache_enabled else 'Desabilitado'}")
        logger.info(f"   • Cache version: {settings.etl_cache_version}")
        logger.info("=" * 80)
        
        # 3. Carregar lista de CNPJs
        input_file = settings.get_input_file()
        df_cnpjs = carregar_cnpjs(input_file)
        
        # Estimativa de tempo
        docs_estimados = 40  # Média de meses com dados por CNPJ
        tempo_seq = len(df_cnpjs) * settings.b3_delay_requisicoes * docs_estimados / 60
        tempo_par = tempo_seq / settings.etl_max_workers
        logger.info(f"⏱️ Tempo estimado: ~{tempo_par:.1f} min ({settings.etl_max_workers} threads)")
        
        # 4. Processar CNPJs com ThreadPoolExecutor
        logger.info("=" * 80)
        logger.info("🚀 INICIANDO PROCESSAMENTO PARALELO")
        logger.info("=" * 80)
        
        tempo_inicio = datetime.now()
        todos_resultados = []

        with ThreadPoolExecutor(max_workers=settings.etl_max_workers) as executor:
            future_to_cnpj = {}
            for _, row in df_cnpjs.iterrows():
                future = executor.submit(
                    pipeline_por_cnpj,
                    row['CNPJ'],
                    row.get('NOME_FUNDO', ''),
                    api, cache, extractor
                )
                future_to_cnpj[future] = row['CNPJ']
            
            total = len(future_to_cnpj)
            completed = 0
            
            for future in as_completed(future_to_cnpj):
                cnpj = future_to_cnpj[future]
                completed += 1
                
                try:
                    resultados_cnpj = future.result()
                    if resultados_cnpj:
                        todos_resultados.extend(resultados_cnpj)
                        sucessos = sum(1 for r in resultados_cnpj if r.get('STATUS') == 'SUCESSO')
                        logger.info(
                            f"[{completed}/{total}] CNPJ {cnpj}: "
                            f"{sucessos}/{len(resultados_cnpj)} docs OK"
                        )
                    else:
                        todos_resultados.append({
                            'CNPJ_FUNDO': cnpj,
                            'STATUS': 'NENHUM_DOCUMENTO',
                            'TIPO_COLETA': 'ERRO'
                        })
                except Exception as e:
                    logger.error(f"[{completed}/{total}] Erro CNPJ {cnpj}: {e}")
        
        tempo_decorrido = (datetime.now() - tempo_inicio).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"✅ Processamento concluído em {tempo_decorrido/60:.2f} minutos")
        logger.info(f"📊 Total de registros: {len(todos_resultados)}")
        
        # 5. Análise de erros
        _log_error_summary(todos_resultados)
        
        # 6. Exportar resultados
        exporter = CSVExporter()
        exporter.export_monitor_completo(todos_resultados, timestamp)
        exporter.export_distressed_npl(todos_resultados, 20.0, timestamp)
        exporter.export_safari_oportunidades(
            todos_resultados, settings.etl_score_min, timestamp
        )
        
        # 7. Estatísticas finais
        _log_final_stats(todos_resultados)
        
        logger.info("=" * 80)
        logger.info("🎉 Pipeline concluído com sucesso!")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Erro fatal no pipeline: {e}", exc_info=True)
        raise


def _log_error_summary(resultados: List[Dict]) -> None:
    """Loga resumo de erros."""
    df = pd.DataFrame(resultados)
    if df.empty:
        return
    
    erros = df[df.get('STATUS', pd.Series()) != 'SUCESSO']
    if hasattr(erros, 'STATUS') and not erros.empty:
        erros_por_tipo = erros['STATUS'].value_counts()
        logger.info("\n⚠️ RESUMO DE ERROS:")
        for tipo, qtd in erros_por_tipo.items():
            logger.info(f"   {tipo}: {qtd}")


def _log_final_stats(resultados: List[Dict]) -> None:
    """Loga estatísticas finais."""
    df = pd.DataFrame(resultados)
    if df.empty:
        return
    
    df_valido = df[df.get('STATUS', pd.Series()) == 'SUCESSO'] if 'STATUS' in df.columns else df
    
    if df_valido.empty:
        logger.warning("Nenhum registro válido para estatísticas")
        return
    
    # Converter colunas numéricas
    for col in ['NPL_RATIO', 'ZUMBI_RATIO', 'ATIVO_TOTAL', 'CARTEIRA_TOTAL']:
        if col in df_valido.columns:
            df_valido[col] = pd.to_numeric(df_valido[col], errors='coerce')
    
    logger.info("\n📊 ESTATÍSTICAS DO MERCADO:")
    if 'CNPJ_FUNDO' in df_valido.columns:
        logger.info(f"   🏦 Fundos únicos: {df_valido['CNPJ_FUNDO'].nunique()}")
    if 'ATIVO_TOTAL' in df_valido.columns:
        logger.info(f"   💰 Ativos: R$ {df_valido['ATIVO_TOTAL'].sum()/1e9:.2f} bi")
    if 'NPL_RATIO' in df_valido.columns:
        logger.info(f"   📊 NPL médio: {df_valido['NPL_RATIO'].mean():.2f}%")
    if 'ZUMBI_RATIO' in df_valido.columns:
        logger.info(f"   🧟 Zumbi Ratio médio: {df_valido['ZUMBI_RATIO'].mean():.2f}%")


if __name__ == "__main__":
    main()

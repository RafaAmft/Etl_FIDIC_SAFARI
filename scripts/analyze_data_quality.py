"""
Script de Análise de Qualidade de Dados FIDC.

Gera relatório completo de qualidade dos dados coletados:
- Estatísticas gerais
- Análise de flags de QA
- Indicadores financeiros
- Completude de dados
- Detecção de outliers

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logging_config import setup_logging
from src.config import settings


def analyze_general_stats(df: pd.DataFrame) -> dict:
    """Analisa estatísticas gerais do dataset."""
    stats = {
        'total_registros': len(df),
        'total_sucesso': (df['STATUS'] == 'SUCESSO').sum(),
        'total_erros': (df['STATUS'] != 'SUCESSO').sum(),
        'taxa_sucesso': (df['STATUS'] == 'SUCESSO').sum() / len(df) * 100,
        'colunas_totais': len(df.columns),
        'data_execucao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    return stats


def analyze_qa_flags(df: pd.DataFrame) -> dict:
    """Analisa as flags de validação de QA."""
    df_sucesso = df[df['STATUS'] == 'SUCESSO'].copy()
    
    flags = {
        'ATIVO_ZERO_FLAG': df_sucesso['ATIVO_ZERO_FLAG'].sum() if 'ATIVO_ZERO_FLAG' in df_sucesso.columns else 0,
        'DIVERGE_LIQ_FLAG': df_sucesso['DIVERGE_LIQ_FLAG'].sum() if 'DIVERGE_LIQ_FLAG' in df_sucesso.columns else 0,
        'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG': df_sucesso['CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG'].sum() if 'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG' in df_sucesso.columns else 0,
        'DIVERGE_NPL_FLAG': df_sucesso['DIVERGE_NPL_FLAG'].sum() if 'DIVERGE_NPL_FLAG' in df_sucesso.columns else 0,
        'SEM_POSICAO_FLAG': df_sucesso['SEM_POSICAO_FLAG'].sum() if 'SEM_POSICAO_FLAG' in df_sucesso.columns else 0,
    }
    
    flags['total_com_issues'] = sum(flags.values())
    flags['taxa_qualidade'] = (len(df_sucesso) - flags['total_com_issues']) / len(df_sucesso) * 100 if len(df_sucesso) > 0 else 0
    
    return flags


def analyze_financial_indicators(df: pd.DataFrame) -> dict:
    """Analisa indicadores financeiros."""
    df_sucesso = df[df['STATUS'] == 'SUCESSO'].copy()
    
    # Converter para numérico se necessário
    numeric_cols = ['ATIVO_TOTAL', 'INADIMPLENCIA_TOTAL', 'INDICE_NPL_DECIMAL', 
                    'TAXA_LIQUIDEZ_DECIMAL', 'CARTEIRA_BRUTA']
    
    for col in numeric_cols:
        if col in df_sucesso.columns:
            df_sucesso[col] = pd.to_numeric(df_sucesso[col], errors='coerce')
    
    stats = {}
    
    # Ativo Total
    if 'ATIVO_TOTAL' in df_sucesso.columns:
        stats['ativo_total'] = {
            'soma': df_sucesso['ATIVO_TOTAL'].sum(),
            'media': df_sucesso['ATIVO_TOTAL'].mean(),
            'mediana': df_sucesso['ATIVO_TOTAL'].median(),
            'min': df_sucesso['ATIVO_TOTAL'].min(),
            'max': df_sucesso['ATIVO_TOTAL'].max(),
        }
    
    # NPL
    if 'INDICE_NPL_DECIMAL' in df_sucesso.columns:
        npl_validos = df_sucesso['INDICE_NPL_DECIMAL'].dropna()
        stats['npl'] = {
            'media': npl_validos.mean() * 100,  # Converter para percentual
            'mediana': npl_validos.median() * 100,
            'min': npl_validos.min() * 100,
            'max': npl_validos.max() * 100,
            'com_npl': (npl_validos > 0).sum(),
            'sem_npl': (npl_validos == 0).sum(),
        }
    
    # Liquidez
    if 'TAXA_LIQUIDEZ_DECIMAL' in df_sucesso.columns:
        liq_validos = df_sucesso['TAXA_LIQUIDEZ_DECIMAL'].dropna()
        stats['liquidez'] = {
            'media': liq_validos.mean() * 100,
            'mediana': liq_validos.median() * 100,
            'min': liq_validos.min() * 100,
            'max': liq_validos.max() * 100,
        }
    
    return stats


def analyze_data_completeness(df: pd.DataFrame) -> dict:
    """Analisa completude dos dados (campos nulos/vazios)."""
    df_sucesso = df[df['STATUS'] == 'SUCESSO'].copy()
    
    # Campos importantes para análise
    campos_importantes = [
        'ATIVO_TOTAL', 'DISPONIBILIDADES', 'CARTEIRA_TOTAL',
        'CREDITOS_ADQUIRIDOS', 'DICRED_TOTAL', 'INADIMPLENCIA_TOTAL',
        'INDICE_NPL_DECIMAL', 'TAXA_LIQUIDEZ_DECIMAL'
    ]
    
    completeness = {}
    
    for campo in campos_importantes:
        if campo in df_sucesso.columns:
            total = len(df_sucesso)
            nulos = df_sucesso[campo].isna().sum()
            zeros = (df_sucesso[campo] == 0).sum()
            
            completeness[campo] = {
                'total': total,
                'nulos': nulos,
                'zeros': zeros,
                'preenchidos': total - nulos,
                'taxa_completude': (total - nulos) / total * 100 if total > 0 else 0,
            }
    
    return completeness


def detect_outliers(df: pd.DataFrame) -> dict:
    """Detecta outliers e valores suspeitos."""
    df_sucesso = df[df['STATUS'] == 'SUCESSO'].copy()
    
    outliers = {}
    
    # NPL > 50% (muito alto)
    if 'INDICE_NPL_DECIMAL' in df_sucesso.columns:
        npl_alto = df_sucesso[df_sucesso['INDICE_NPL_DECIMAL'] > 0.5]
        outliers['npl_alto'] = {
            'count': len(npl_alto),
            'cnpjs': npl_alto['CNPJ_FUNDO'].tolist()[:10]  # Primeiros 10
        }
    
    # Ativo Total muito baixo (< 100k)
    if 'ATIVO_TOTAL' in df_sucesso.columns:
        ativo_baixo = df_sucesso[df_sucesso['ATIVO_TOTAL'] < 100000]
        outliers['ativo_baixo'] = {
            'count': len(ativo_baixo),
            'cnpjs': ativo_baixo['CNPJ_FUNDO'].tolist()[:10]
        }
    
    # Ativo Total muito alto (> 1 bilhão)
    if 'ATIVO_TOTAL' in df_sucesso.columns:
        ativo_alto = df_sucesso[df_sucesso['ATIVO_TOTAL'] > 1_000_000_000]
        outliers['ativo_alto'] = {
            'count': len(ativo_alto),
            'cnpjs': ativo_alto['CNPJ_FUNDO'].tolist()[:10]
        }
    
    return outliers


def get_top_fundos(df: pd.DataFrame, n: int = 10) -> dict:
    """Obtém top N fundos por diferentes métricas."""
    df_sucesso = df[df['STATUS'] == 'SUCESSO'].copy()
    
    tops = {}
    
    # Top NPL
    if 'INDICE_NPL_DECIMAL' in df_sucesso.columns:
        top_npl = df_sucesso.nlargest(n, 'INDICE_NPL_DECIMAL')[
            ['CNPJ_FUNDO', 'INDICE_NPL_DECIMAL', 'ATIVO_TOTAL']
        ].copy()
        top_npl['INDICE_NPL_PERCENTUAL'] = top_npl['INDICE_NPL_DECIMAL'] * 100
        tops['top_npl'] = top_npl.to_dict('records')
    
    # Top Ativo Total
    if 'ATIVO_TOTAL' in df_sucesso.columns:
        top_ativo = df_sucesso.nlargest(n, 'ATIVO_TOTAL')[
            ['CNPJ_FUNDO', 'ATIVO_TOTAL', 'INDICE_NPL_DECIMAL']
        ].copy()
        tops['top_ativo'] = top_ativo.to_dict('records')
    
    return tops


def generate_report(df: pd.DataFrame) -> str:
    """Gera relatório textual completo."""
    
    report = []
    report.append("=" * 80)
    report.append("📊 RELATÓRIO DE QUALIDADE DE DADOS - FIDC")
    report.append("=" * 80)
    report.append("")
    
    # 1. Estatísticas Gerais
    stats = analyze_general_stats(df)
    report.append("## 1. ESTATÍSTICAS GERAIS")
    report.append("-" * 80)
    report.append(f"   Total de registros: {stats['total_registros']:,}")
    report.append(f"   ✅ Sucesso: {stats['total_sucesso']:,} ({stats['taxa_sucesso']:.1f}%)")
    report.append(f"   ❌ Erros: {stats['total_erros']:,}")
    report.append(f"   Colunas no dataset: {stats['colunas_totais']}")
    report.append(f"   Data da análise: {stats['data_execucao']}")
    report.append("")
    
    # 2. Análise de QA Flags
    qa_flags = analyze_qa_flags(df)
    report.append("## 2. ANÁLISE DE QUALIDADE (FLAGS QA)")
    report.append("-" * 80)
    report.append(f"   Total com issues: {qa_flags['total_com_issues']:,}")
    report.append(f"   Taxa de qualidade: {qa_flags['taxa_qualidade']:.1f}%")
    report.append("")
    report.append("   Detalhamento por flag:")
    for flag, count in qa_flags.items():
        if flag not in ['total_com_issues', 'taxa_qualidade']:
            report.append(f"      • {flag}: {count:,}")
    report.append("")
    
    # 3. Indicadores Financeiros
    fin_stats = analyze_financial_indicators(df)
    report.append("## 3. INDICADORES FINANCEIROS")
    report.append("-" * 80)
    
    if 'ativo_total' in fin_stats:
        report.append("   ATIVO TOTAL:")
        report.append(f"      Soma: R$ {fin_stats['ativo_total']['soma']:,.2f}")
        report.append(f"      Média: R$ {fin_stats['ativo_total']['media']:,.2f}")
        report.append(f"      Mediana: R$ {fin_stats['ativo_total']['mediana']:,.2f}")
        report.append(f"      Min: R$ {fin_stats['ativo_total']['min']:,.2f}")
        report.append(f"      Max: R$ {fin_stats['ativo_total']['max']:,.2f}")
        report.append("")
    
    if 'npl' in fin_stats:
        report.append("   NPL (Non-Performing Loans):")
        report.append(f"      Média: {fin_stats['npl']['media']:.2f}%")
        report.append(f"      Mediana: {fin_stats['npl']['mediana']:.2f}%")
        report.append(f"      Min: {fin_stats['npl']['min']:.2f}%")
        report.append(f"      Max: {fin_stats['npl']['max']:.2f}%")
        report.append(f"      Fundos com NPL > 0: {fin_stats['npl']['com_npl']:,}")
        report.append(f"      Fundos com NPL = 0: {fin_stats['npl']['sem_npl']:,}")
        report.append("")
    
    if 'liquidez' in fin_stats:
        report.append("   LIQUIDEZ:")
        report.append(f"      Média: {fin_stats['liquidez']['media']:.2f}%")
        report.append(f"      Mediana: {fin_stats['liquidez']['mediana']:.2f}%")
        report.append("")
    
    # 4. Completude de Dados
    completeness = analyze_data_completeness(df)
    report.append("## 4. COMPLETUDE DE DADOS")
    report.append("-" * 80)
    for campo, stats in completeness.items():
        report.append(f"   {campo}:")
        report.append(f"      Preenchidos: {stats['preenchidos']:,}/{stats['total']:,} ({stats['taxa_completude']:.1f}%)")
        if stats['nulos'] > 0:
            report.append(f"      ⚠️ Nulos: {stats['nulos']:,}")
    report.append("")
    
    # 5. Outliers
    outliers = detect_outliers(df)
    report.append("## 5. DETECÇÃO DE OUTLIERS")
    report.append("-" * 80)
    
    if 'npl_alto' in outliers:
        report.append(f"   ⚠️ Fundos com NPL > 50%: {outliers['npl_alto']['count']:,}")
    
    if 'ativo_baixo' in outliers:
        report.append(f"   ⚠️ Fundos com Ativo < R$ 100k: {outliers['ativo_baixo']['count']:,}")
    
    if 'ativo_alto' in outliers:
        report.append(f"   ✅ Fundos com Ativo > R$ 1bi: {outliers['ativo_alto']['count']:,}")
    report.append("")
    
    # 6. Top Fundos
    tops = get_top_fundos(df, n=5)
    report.append("## 6. TOP 5 FUNDOS POR NPL")
    report.append("-" * 80)
    
    if 'top_npl' in tops:
        for i, fundo in enumerate(tops['top_npl'], 1):
            npl_pct = fundo.get('INDICE_NPL_PERCENTUAL', 0)
            ativo = fundo.get('ATIVO_TOTAL', 0)
            report.append(f"   {i}. CNPJ: {fundo['CNPJ_FUNDO']}")
            report.append(f"      NPL: {npl_pct:.2f}% | Ativo: R$ {ativo:,.2f}")
    
    report.append("")
    report.append("=" * 80)
    report.append("✅ RELATÓRIO CONCLUÍDO")
    report.append("=" * 80)
    
    return "\n".join(report)


def main():
    """Executa análise de qualidade de dados."""
    
    # Configurar logging
    setup_logging(log_level="INFO")
    logger = logging.getLogger(__name__)
    
    print("=" * 80)
    print("📊 Análise de Qualidade de Dados - FIDC")
    print("=" * 80)
    print()
    
    # Verificar arquivo
    arquivo_entrada = settings.OUTPUTS_DIR / "cleaned_snapshot.csv"
    
    if not arquivo_entrada.exists():
        logger.error(f"Arquivo não encontrado: {arquivo_entrada}")
        print(f"\n❌ ERRO: Arquivo '{arquivo_entrada}' não encontrado!")
        print(f"\nExecute primeiro: python scripts/run_etl.py")
        return 1
    
    try:
        # Carregar dados
        logger.info(f"Carregando dados de {arquivo_entrada}")
        df = pd.read_csv(arquivo_entrada, sep=';', decimal=',')
        
        print(f"✅ Dados carregados: {len(df):,} registros\n")
        
        # Gerar relatório
        logger.info("Gerando relatório de qualidade...")
        report = generate_report(df)
        
        # Exibir no console
        print(report)
        
        # Salvar em arquivo
        output_file = settings.OUTPUTS_DIR / "data_quality_report.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n📁 Relatório salvo em: {output_file}")
        
        logger.info("Análise de qualidade concluída com sucesso!")
        return 0
        
    except Exception as e:
        logger.exception("Erro durante análise de qualidade")
        print(f"\n❌ ERRO: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

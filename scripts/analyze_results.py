"""
Análise do Monitor Completo FIDC ETL
"""
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Configurar encoding para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Carregar dados
file_path = Path("data/output/2025-12-27/fidc_monitor_completo_20251226_232810.csv")
df = pd.read_csv(file_path)

print("="*80)
print("ANÁLISE DO MONITOR COMPLETO - FIDC ETL V7.0")
print("="*80)
print()

# 1. VISÃO GERAL
print("📊 1. VISÃO GERAL")
print("-" * 80)
print(f"Total de documentos processados: {len(df):,}")
print(f"Total de fundos únicos (CNPJs): {df['CNPJ_FUNDO'].nunique():,}")
print(f"Período de dados: {df['DATA_COMPETENCIA'].min()} a {df['DATA_COMPETENCIA'].max()}")
print(f"Colunas disponíveis: {len(df.columns)}")
print()

# 2. DISTRIBUIÇÃO DE NPL
print("📈 2. DISTRIBUIÇÃO DE NPL (Non-Performing Loans)")
print("-" * 80)
npl_stats = df['NPL_RATIO'].describe()
print(f"Média NPL: {npl_stats['mean']:.2f}%")
print(f"Mediana NPL: {npl_stats['50%']:.2f}%")
print(f"NPL Mínimo: {npl_stats['min']:.2f}%")
print(f"NPL Máximo: {npl_stats['max']:.2f}%")
print(f"Desvio Padrão: {npl_stats['std']:.2f}%")
print()

# Classificação por faixa de NPL
print("Distribuição por Faixa de NPL:")
npl_bins = [0, 5, 15, 50, 100, float('inf')]
npl_labels = ['Saudável (0-5%)', 'Atenção (5-15%)', 'Distressed (15-50%)', 
              'Severo (50-100%)', 'Crítico (>100%)']
df['NPL_FAIXA'] = pd.cut(df['NPL_RATIO'], bins=npl_bins, labels=npl_labels)
npl_dist = df['NPL_FAIXA'].value_counts().sort_index()
for faixa, count in npl_dist.items():
    pct = (count / len(df)) * 100
    print(f"  {faixa}: {count:,} ({pct:.1f}%)")
print()

# 3. SAFARI SCORE
print("🎯 3. FIDC SAFARI SCORE")
print("-" * 80)
safari_stats = df['SCORE_SAFARI'].describe()
print(f"Média Score: {safari_stats['mean']:.2f}")
print(f"Mediana Score: {safari_stats['50%']:.2f}")
print(f"Score Máximo: {safari_stats['max']:.2f}")
print()

print("Distribuição por Classificação Safari:")
safari_dist = df['CLASSIFICACAO_SAFARI'].value_counts()
for classif, count in safari_dist.items():
    pct = (count / len(df)) * 100
    print(f"  {classif}: {count:,} ({pct:.1f}%)")
print()

# 4. INDICADORES FINANCEIROS
print("💰 4. INDICADORES FINANCEIROS (em R$ milhões)")
print("-" * 80)
financeiros = {
    'Ativo Total': df['ATIVO_TOTAL'].sum() / 1_000_000,
    'Patrimônio Líquido': df['PATRIMONIO_LIQUIDO'].sum() / 1_000_000,
    'Carteira Total': df['CARTEIRA_TOTAL'].sum() / 1_000_000,
    'Disponibilidades': df['DISPONIBILIDADES'].sum() / 1_000_000,
}

for indicador, valor in financeiros.items():
    print(f"{indicador}: R$ {valor:,.2f} Mi")
print()

# Médias por fundo
print("Médias por Fundo:")
medias = {
    'Ativo Médio': df['ATIVO_TOTAL'].mean() / 1_000_000,
    'PL Médio': df['PATRIMONIO_LIQUIDO'].mean() / 1_000_000,
    'Carteira Média': df['CARTEIRA_TOTAL'].mean() / 1_000_000,
}

for indicador, valor in medias.items():
    print(f"  {indicador}: R$ {valor:,.2f} Mi")
print()

# 5. LIQUIDEZ
print("💧 5. LIQUIDEZ IMEDIATA")
print("-" * 80)
liq_stats = df['LIQUIDEZ_IMEDIATA_RATIO'].describe()
print(f"Média: {liq_stats['mean']:.2f}")
print(f"Mediana: {liq_stats['50%']:.2f}")
print(f"Fundos com Liquidez < 1.0: {(df['LIQUIDEZ_IMEDIATA_RATIO'] < 1.0).sum():,} "
      f"({(df['LIQUIDEZ_IMEDIATA_RATIO'] < 1.0).sum() / len(df) * 100:.1f}%)")
print()

# 6. ZUMBI RATIO
print("🧟 6. ZUMBI RATIO (Créditos > 3 anos)")
print("-" * 80)
zumbi_stats = df['ZUMBI_RATIO'].describe()
print(f"Média: {zumbi_stats['mean']:.2f}%")
print(f"Mediana: {zumbi_stats['50%']:.2f}%")
print(f"Fundos com Zumbi > 20%: {(df['ZUMBI_RATIO'] > 20).sum():,} "
      f"({(df['ZUMBI_RATIO'] > 20).sum() / len(df) * 100:.1f}%)")
print()

# 7. ANOMALIAS
print("⚠️  7. ANOMALIAS ESTRUTURAIS")
print("-" * 80)
anomalias = df['ANOMALIA_ESTRUTURAL'].sum()
print(f"Total de fundos com anomalias: {anomalias:,} ({anomalias/len(df)*100:.1f}%)")
print()

status_dist = df['STATUS_DADOS'].value_counts()
print("Distribuição por Status de Dados:")
for status, count in status_dist.items():
    pct = (count / len(df)) * 100
    print(f"  {status}: {count:,} ({pct:.1f}%)")
print()

# 8. TOP 10 OPORTUNIDADES
print("🏆 8. TOP 10 OPORTUNIDADES (Maior Score Safari)")
print("-" * 80)
top_10 = df.nlargest(10, 'SCORE_SAFARI')[['CNPJ_FUNDO', 'SCORE_SAFARI', 'NPL_RATIO', 
                                            'ATIVO_TOTAL', 'CLASSIFICACAO_SAFARI']]
top_10['ATIVO_MILHOES'] = top_10['ATIVO_TOTAL'] / 1_000_000

print(f"{'#':<4} {'CNPJ':<18} {'Score':<8} {'NPL%':<8} {'Ativo (Mi)':<12} {'Classif':<20}")
print("-" * 80)
for i, row in enumerate(top_10.iterrows(), 1):
    idx, data = row
    cnpj = str(data['CNPJ_FUNDO']).replace('.0', '') if pd.notna(data['CNPJ_FUNDO']) else 'N/A'
    print(f"{i:<4} {cnpj:<18} {data['SCORE_SAFARI']:<8.2f} {data['NPL_RATIO']:<8.2f} "
          f"R$ {data['ATIVO_MILHOES']:<8.2f} {data['CLASSIFICACAO_SAFARI']:<20}")
print()

# 9. RESUMO EXECUTIVO
print("📝 9. RESUMO EXECUTIVO")
print("=" * 80)
total_distressed = (df['NPL_RATIO'] >= 15).sum()
total_oportunidades = (df['SCORE_SAFARI'] >= 50).sum()
ativo_distressed = df[df['NPL_RATIO'] >= 15]['ATIVO_TOTAL'].sum() / 1_000_000

print(f"✓ {len(df):,} documentos processados de {df['CNPJ_FUNDO'].nunique():,} fundos")
print(f"✓ {total_distressed:,} fundos distressed (NPL ≥ 15%)")
print(f"✓ {total_oportunidades:,} oportunidades identificadas (Score ≥ 50)")
print(f"✓ R$ {ativo_distressed:,.2f} Mi em ativos distressed")
print(f"✓ NPL médio do mercado: {df['NPL_RATIO'].mean():.2f}%")
print(f"✓ {anomalias:,} fundos com anomalias estruturais detectadas")
print()
print("="*80)
print("Análise concluída! ✅")
print("="*80)

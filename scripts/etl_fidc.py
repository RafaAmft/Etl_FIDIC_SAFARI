"""
ETL FIDC - Extração de Dados de Fundos FIDC da B3
Author: Rafael Augusto
Date: Janeiro 2026

Executa ETL completo de 441 CNPJs de fundos FIDC extraindo 90+ campos
da API B3 e gerando relatórios estruturados em CSV e Excel.
"""

import sys
import os

# Adiciona o diretório raiz ao path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 80)
print("🚀 ETL FIDC - Iniciando Execução")
print("=" * 80)
print()

# Verificar dependências
print("📦 Verificando dependências...")
try:
    import pandas as pd
    import requests
    import base64
    import xml.etree.ElementTree as ET
    import time
    from typing import Dict, List, Optional
    print("✅ Todas as dependências instaladas")
except ImportError as e:
    print(f"❌ ERRO: Faltam dependências: {e}")
    print("\nInstale com: pip install pandas requests openpyxl")
    sys.exit(1)

# Configurar caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")
OUTPUTS_DIR = os.path.join(PROJECT_DIR, "outputs")

ARQUIVO_CSV_ENTRADA = os.path.join(DATA_DIR, "lista_cnpjs_fidc.csv")

print(f"\n📁 Diretórios:")
print(f"   Projeto: {PROJECT_DIR}")
print(f"   Entrada: {DATA_DIR}")
print(f"   Saída: {OUTPUTS_DIR}")

# Verificar arquivo de entrada
if not os.path.exists(ARQUIVO_CSV_ENTRADA):
    print(f"\n❌ ERRO: Arquivo '{ARQUIVO_CSV_ENTRADA}' não encontrado!")
    sys.exit(1)

print(f"\n✅ Arquivo de entrada encontrado: {ARQUIVO_CSV_ENTRADA}")

# Configurações da API B3
URL_API_BUSCA = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
URL_API_DOWNLOAD = "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"
HEADERS = {"User-Agent": "Mozilla/5.0"}

DELAY_ENTRE_REQUISICOES = 2
TIMEOUT_BUSCA = 10
TIMEOUT_DOWNLOAD = 20

print(f"\n⚙️  Configurações API:")
print(f"   Delay entre requisições: {DELAY_ENTRE_REQUISICOES}s")
print(f"   Timeout busca: {TIMEOUT_BUSCA}s")
print(f"   Timeout download: {TIMEOUT_DOWNLOAD}s")

# Funções auxiliares (copiadas do notebook)
def limpar_tag(tag: str) -> str:
    return tag.split('}')[-1] if '}' in tag else tag

def converter_valor(texto: str) -> float:
    if not texto or not str(texto).strip():
        return 0.0
    try:
        texto_limpo = str(texto).replace('.', '').replace(',', '.')
        return float(texto_limpo)
    except (ValueError, AttributeError):
        return 0.0

def buscar_valor_xml(root: ET.Element, caminho: str):
    elemento = root.find(f'.//{caminho}')
    if elemento is not None and elemento.text:
        try:
            return converter_valor(elemento.text)
        except:
            return elemento.text.strip()
    return 0.0 if '/' in caminho else ''

# (Restante das funções será copiado do notebook...)

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("⚠️  ATENÇÃO: Este script está em preparação")
    print("=" * 80)
    print("\nPara executar o ETL completo, use o Jupyter Notebook:")
    print(f"   jupyter notebook {os.path.join(PROJECT_DIR, 'notebooks', 'etl_fidic_vfinal.ipynb')}")
    print("\nAlternativamente, converta o notebook completo para este script.")
    print()

"""
Configurações do ETL FIDC.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import os
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════
# CAMINHOS DO PROJETO
# ═══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUTS_DIR = BASE_DIR / "outputs"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"

# ═══════════════════════════════════════════════════════════════════════════
# API B3
# ═══════════════════════════════════════════════════════════════════════════

URL_API_BUSCA = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
URL_API_DOWNLOAD = "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ═══════════════════════════════════════════════════════════════════════════
# TIMEOUTS E DELAYS
# ═══════════════════════════════════════════════════════════════════════════

DELAY_ENTRE_REQUISICOES = 2  # segundos
TIMEOUT_BUSCA = 10  # segundos
TIMEOUT_DOWNLOAD = 20  # segundos
LIMITE_DOCS = 200  # documentos a buscar

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES DE EXPORTAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

EXPORT_ENCODING = 'utf-8-sig'
EXPORT_SEP = ';'
EXPORT_DECIMAL = ','
FLOAT_FORMAT = '%.2f'

# ═══════════════════════════════════════════════════════════════════════════
# VALIDAÇÕES
# ═══════════════════════════════════════════════════════════════════════════

# Threshold para divergência de NPL
NPL_DIFF_THRESHOLD = 0.01

# Threshold para divergência de Liquidez (em casas decimais)
LIQUIDEZ_DECIMAL_PLACES = 2

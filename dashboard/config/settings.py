"""
Configurações globais do dashboard FIDC Monitor.
"""
from pathlib import Path
import os

# Paths - Detectar ambiente (local vs cloud)
DASHBOARD_ROOT = Path(__file__).parent.parent

# Tentar primeiro o caminho dentro do dashboard (para cloud)
# Se não existir, tentar o caminho do projeto pai (para local)
DATA_DIR_DASHBOARD = DASHBOARD_ROOT / "data"
DATA_DIR_PROJECT = DASHBOARD_ROOT.parent / "outputs"

if (DATA_DIR_DASHBOARD / "cleaned_snapshot.csv").exists():
    DATA_DIR = DATA_DIR_DASHBOARD
    DEFAULT_DATA_FILE = DATA_DIR / "cleaned_snapshot.csv"
elif (DATA_DIR_PROJECT / "cleaned_snapshot_latest.csv").exists():
    DATA_DIR = DATA_DIR_PROJECT
    DEFAULT_DATA_FILE = DATA_DIR / "cleaned_snapshot_latest.csv"
else:
    # Fallback para cloud
    DATA_DIR = DATA_DIR_DASHBOARD
    DEFAULT_DATA_FILE = DATA_DIR / "cleaned_snapshot.csv"

# Colunas obrigatórias
REQUIRED_COLUMNS = [
    'CNPJ_FUNDO',
    'DATA_COMPETENCIA',
    'ATIVO_TOTAL',
    'CARTEIRA_TOTAL',
    'INADIMPLENCIA_TOTAL',
    'INDICE_NPL_DECIMAL',
    'STATUS'
]

# Colunas de flags de QA
FLAG_COLUMNS = [
    'ATIVO_ZERO_FLAG',
    'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG',
    'SEM_POSICAO_FLAG'
]

# Colunas de segmentação (prefixo SEGMT_)
SEGMENT_PREFIX = 'SEGMT_'

# Formatação
CURRENCY_FORMAT = "R$ {:,.2f}"
PERCENT_FORMAT = "{:.2f}%"
NUMBER_FORMAT = "{:,.0f}"

# Thresholds
NPL_WARNING_THRESHOLD = 0.10  # 10%
NPL_CRITICAL_THRESHOLD = 0.50  # 50%
LIQUIDITY_WARNING_THRESHOLD = 0.05  # 5%

# Plotly config
PLOTLY_CONFIG = {
    'displayModeBar': True,
    'displaylogo': False,
    'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
    'toImageButtonOptions': {
        'format': 'png',
        'filename': 'grafico_fidc',
        'height': 800,
        'width': 1200,
        'scale': 2
    }
}

PLOTLY_LAYOUT = {
    'template': 'plotly_white',
    'font': {'family': 'Arial, sans-serif', 'size': 12},
    'hovermode': 'x unified',
    'showlegend': True,
    'margin': dict(l=40, r=40, t=60, b=40)
}

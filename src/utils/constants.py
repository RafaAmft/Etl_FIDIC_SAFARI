"""
Constantes de negócio do projeto FIDC ETL.

Centraliza todos os thresholds e valores fixos para evitar magic numbers
espalhados pelo código.
"""

# ---------------------------------------------------------------------------
# Scoring Safari
# ---------------------------------------------------------------------------
NPL_DISTRESSED_MIN: float = 15.0       # % mínimo de NPL para considerar distressed
NPL_DISTRESSED_MAX: float = 150.0      # % máximo (acima = risco excessivo)
NPL_OUTLIER_THRESHOLD: float = 1000.0  # % — acima disso é provavelmente erro de dados
ZUMBI_SCORE_DIVISOR: float = 50.0      # Divisor para normalizar o Zumbi Ratio no score

# Pesos dos componentes do score Safari (somam 100)
PESO_NPL: float = 40.0
PESO_ZUMBI: float = 25.0
PESO_LIQUIDEZ: float = 25.0
PESO_PORTE: float = 10.0

# ---------------------------------------------------------------------------
# Classificação de score Safari
# ---------------------------------------------------------------------------
SCORE_ALTA_OPORTUNIDADE: float = 70.0
SCORE_OPORTUNIDADE_MODERADA: float = 50.0
SCORE_MONITORAR: float = 30.0
SCORE_BAIXA_PRIORIDADE: float = 15.0

# Labels de classificação
LABEL_ALTA_OPORTUNIDADE: str = "ALTA_OPORTUNIDADE"
LABEL_OPORTUNIDADE_MODERADA: str = "OPORTUNIDADE_MODERADA"
LABEL_MONITORAR: str = "MONITORAR"
LABEL_BAIXA_PRIORIDADE: str = "BAIXA_PRIORIDADE"
LABEL_SEM_OPORTUNIDADE: str = "SEM_OPORTUNIDADE"
LABEL_DADOS_INVALIDOS: str = "DADOS_INVALIDOS"

# ---------------------------------------------------------------------------
# Thresholds de porte de fundo (Ativo Total em R$)
# ---------------------------------------------------------------------------
ATIVO_GRANDE_PORTE: float = 100_000_000.0   # >= R$ 100M → 10 pontos de porte
ATIVO_MEDIO_PORTE: float = 50_000_000.0     # >= R$ 50M  → 7 pontos
ATIVO_PEQUENO_PORTE: float = 10_000_000.0   # >= R$ 10M  → 5 pontos
ATIVO_MICRO_PORTE: float = 1_000_000.0      # >= R$ 1M   → 2 pontos

PONTOS_GRANDE_PORTE: float = 10.0
PONTOS_MEDIO_PORTE: float = 7.0
PONTOS_PEQUENO_PORTE: float = 5.0
PONTOS_MICRO_PORTE: float = 2.0

# ---------------------------------------------------------------------------
# Detecção de anomalias
# ---------------------------------------------------------------------------
THRESHOLD_CRED_ADQ_BAIXO: float = 0.01      # < 1% da carteira → FLAG_CRED_ADQ_BAIXO
THRESHOLD_VENCIDOS_ALTO: float = 1.5        # > 150% da carteira → FLAG_VENCIDOS_ALTO
THRESHOLD_NPL_IMPLAUSIVEL: float = 5.0      # > 500% → FLAG_NPL_IMPLAUSIVEL (multiplicador)
THRESHOLD_CARTEIRA_ZERADA: float = 1_000.0  # < R$ 1.000 → FLAG_CARTEIRA_ZERADA

# Labels de anomalia
FLAG_CRED_ADQ_BAIXO: str = "FLAG_CRED_ADQ_BAIXO"
FLAG_VENCIDOS_ALTO: str = "FLAG_VENCIDOS_ALTO"
FLAG_NPL_IMPLAUSIVEL: str = "FLAG_NPL_IMPLAUSIVEL"
FLAG_CARTEIRA_ZERADA: str = "FLAG_CARTEIRA_ZERADA"

STATUS_VALIDADO: str = "VALIDADO"
STATUS_ANOMALIA_ESTRUTURAL: str = "ANOMALIA_ESTRUTURAL"
STATUS_ANOMALIA_CONTABIL: str = "ANOMALIA_CONTABIL"
STATUS_NPL_IMPLAUSIVEL: str = "NPL_IMPLAUSIVEL"
STATUS_CARTEIRA_ZERADA: str = "CARTEIRA_ZERADA"

# ---------------------------------------------------------------------------
# Cedentes concentrados
# ---------------------------------------------------------------------------
THRESHOLD_CEDENTE_CONCENTRADO: float = 10.0  # % do PL → cedente "concentrado"

# ---------------------------------------------------------------------------
# Aging da carteira (em dias)
# ---------------------------------------------------------------------------
AGING_BUCKET_ZUMBI_DIAS: int = 1080  # Créditos vencidos há > 1080 dias

# ---------------------------------------------------------------------------
# Validação de datas
# ---------------------------------------------------------------------------
YEAR_MIN: int = 2000
YEAR_MAX: int = 2035

# ---------------------------------------------------------------------------
# API B3
# ---------------------------------------------------------------------------
DOCUMENT_TYPE: str = "Informe Mensal Estruturado"
DOCUMENT_STATUS_ATIVO: str = "A"
DOCUMENT_LIMIT: int = 200  # Máximo de documentos por busca

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
DEFAULT_ROLLING_WINDOW_MONTHS: int = 60
DEFAULT_MAX_WORKERS: int = 5
ESTIMATED_DOCS_PER_CNPJ: int = 40  # Estimativa para cálculo de tempo

# ---------------------------------------------------------------------------
# Export CSV
# ---------------------------------------------------------------------------
CNPJ_INVALIDO_VALUES: list[str] = ["", "0", "0.0", "nan", "None"]
CSV_ENCODING: str = "utf-8-sig"  # BOM para compatibilidade com Excel

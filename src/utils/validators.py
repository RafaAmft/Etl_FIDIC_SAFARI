"""
Validadores de entrada com Pydantic v2.

Inclui validação de CNPJ com checksum mod 11 e schemas de dados.
"""
import re
from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator


# ---------------------------------------------------------------------------
# CNPJ — validação completa com dígitos verificadores
# ---------------------------------------------------------------------------

def _cnpj_checksum(cnpj: str) -> bool:
    """
    Valida os dígitos verificadores do CNPJ usando algoritmo mod 11.

    Args:
        cnpj: String com 14 dígitos numéricos (sem formatação).

    Returns:
        True se o CNPJ é válido, False caso contrário.
    """
    # CNPJs com todos os dígitos iguais são inválidos (ex: 00000000000000)
    if len(set(cnpj)) == 1:
        return False

    def calcular_digito(cnpj_parcial: str, pesos: list[int]) -> int:
        total = sum(int(d) * p for d, p in zip(cnpj_parcial, pesos))
        resto = total % 11
        return 0 if resto < 2 else 11 - resto

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    d1 = calcular_digito(cnpj[:12], pesos1)
    d2 = calcular_digito(cnpj[:13], pesos2)

    return cnpj[12] == str(d1) and cnpj[13] == str(d2)


def validate_cnpj(cnpj_raw: str) -> str:
    """
    Remove formatação e valida CNPJ (estrutura + checksum).

    Args:
        cnpj_raw: CNPJ em qualquer formato (ex: "12.345.678/0001-95" ou "12345678000195").

    Returns:
        CNPJ com 14 dígitos numéricos.

    Raises:
        ValueError: Se o CNPJ for inválido.
    """
    cnpj = re.sub(r"\D", "", str(cnpj_raw))

    if len(cnpj) != 14:
        raise ValueError(f"CNPJ deve ter 14 dígitos, recebido {len(cnpj)}: {cnpj_raw!r}")

    if not _cnpj_checksum(cnpj):
        raise ValueError(f"CNPJ com dígitos verificadores inválidos: {cnpj_raw!r}")

    return cnpj


# ---------------------------------------------------------------------------
# Schemas Pydantic v2
# ---------------------------------------------------------------------------

class CNPJInput(BaseModel):
    """Schema de entrada para validação de CNPJ."""

    cnpj: str

    @field_validator("cnpj")
    @classmethod
    def validate_cnpj_field(cls, v: str) -> str:
        return validate_cnpj(v)


class FundoData(BaseModel):
    """
    Schema de saída de um registro de fundo FIDC.

    Campos extras (100+) são permitidos via extra='allow'.
    Apenas os campos críticos são tipados explicitamente.
    """

    CNPJ_FUNDO: str
    DATA_COMPETENCIA: Optional[str] = None
    ATIVO_TOTAL: float = 0.0
    CARTEIRA_TOTAL: float = 0.0
    PATRIMONIO_LIQUIDO: float = 0.0
    DISPONIBILIDADES: float = 0.0
    PASSIVO_CIRCULANTE: float = 0.0

    # Métricas calculadas
    NPL_RATIO: float = 0.0
    ZUMBI_RATIO: float = 0.0
    LIQUIDEZ_IMEDIATA_RATIO: float = 0.0

    # Scoring Safari
    SCORE_SAFARI: float = 0.0
    CLASSIFICACAO_SAFARI: str = ""

    # Anomalias
    ANOMALIA_ESTRUTURAL: bool = False
    STATUS_DADOS: str = ""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ExportFilter(BaseModel):
    """Parâmetros de filtro para exportação."""

    score_min: float = 30.0
    npl_min: float = 20.0
    ativo_min: float = 0.0

    @field_validator("score_min", "npl_min", "ativo_min")
    @classmethod
    def validate_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"Threshold não pode ser negativo: {v}")
        return v

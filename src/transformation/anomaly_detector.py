"""
Detector de anomalias estruturais em fundos FIDC.
"""
import logging
from typing import Any, Dict

from src.utils.constants import (
    FLAG_CRED_ADQ_BAIXO,
    FLAG_CARTEIRA_ZERADA,
    FLAG_NPL_IMPLAUSIVEL,
    FLAG_VENCIDOS_ALTO,
    STATUS_ANOMALIA_CONTABIL,
    STATUS_ANOMALIA_ESTRUTURAL,
    STATUS_CARTEIRA_ZERADA,
    STATUS_NPL_IMPLAUSIVEL,
    STATUS_VALIDADO,
    THRESHOLD_CARTEIRA_ZERADA,
    THRESHOLD_CRED_ADQ_BAIXO,
    THRESHOLD_NPL_IMPLAUSIVEL,
    THRESHOLD_VENCIDOS_ALTO,
)

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detector de anomalias em dados de fundos."""

    @staticmethod
    def detect_anomalies(dados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detecta anomalias estruturais nos dados.

        Args:
            dados: Dicionário com dados do fundo.

        Returns:
            Dict com flags de anomalia e STATUS_DADOS.
        """
        carteira_total = float(dados.get("CARTEIRA_TOTAL", 0) or 0)
        creditos_adquiridos = float(dados.get("CREDITOS_ADQUIRIDOS", 0) or 0)
        creditos_vencidos = float(dados.get("CRED_TOTAL_VENC_INADIMPL", 0) or 0)
        npl_ratio = float(dados.get("NPL_RATIO", 0) or 0)

        anomalias: Dict[str, Any] = {}

        # Flag 1: CREDITOS_ADQUIRIDOS muito baixo (< threshold da carteira)
        if carteira_total > 0:
            ratio_cred_carteira = creditos_adquiridos / carteira_total
            anomalias[FLAG_CRED_ADQ_BAIXO] = ratio_cred_carteira < THRESHOLD_CRED_ADQ_BAIXO
        else:
            anomalias[FLAG_CRED_ADQ_BAIXO] = False

        # Flag 2: Créditos vencidos > threshold da carteira (anomalia contábil)
        if carteira_total > 0:
            anomalias[FLAG_VENCIDOS_ALTO] = (
                creditos_vencidos > carteira_total * THRESHOLD_VENCIDOS_ALTO
            )
        else:
            anomalias[FLAG_VENCIDOS_ALTO] = False

        # Flag 3: NPL implausível (> threshold × 100%)
        anomalias[FLAG_NPL_IMPLAUSIVEL] = npl_ratio > (THRESHOLD_NPL_IMPLAUSIVEL * 100)

        # Flag 4: Carteira total zerada ou muito pequena
        anomalias[FLAG_CARTEIRA_ZERADA] = carteira_total < THRESHOLD_CARTEIRA_ZERADA

        # Flag consolidada: Anomalia estrutural
        anomalias["ANOMALIA_ESTRUTURAL"] = any(
            [
                anomalias[FLAG_CRED_ADQ_BAIXO],
                anomalias[FLAG_VENCIDOS_ALTO],
                anomalias[FLAG_NPL_IMPLAUSIVEL],
                anomalias[FLAG_CARTEIRA_ZERADA],
            ]
        )

        # Status de qualidade dos dados (prioridade: carteira zerada > NPL > contábil > estrutural)
        if not anomalias["ANOMALIA_ESTRUTURAL"]:
            anomalias["STATUS_DADOS"] = STATUS_VALIDADO
        elif anomalias[FLAG_CARTEIRA_ZERADA]:
            anomalias["STATUS_DADOS"] = STATUS_CARTEIRA_ZERADA
        elif anomalias[FLAG_NPL_IMPLAUSIVEL]:
            anomalias["STATUS_DADOS"] = STATUS_NPL_IMPLAUSIVEL
        elif anomalias[FLAG_VENCIDOS_ALTO]:
            anomalias["STATUS_DADOS"] = STATUS_ANOMALIA_CONTABIL
        else:
            anomalias["STATUS_DADOS"] = STATUS_ANOMALIA_ESTRUTURAL

        return anomalias

    @staticmethod
    def add_anomaly_flags(dados: Dict[str, Any]) -> None:
        """
        Detecta e adiciona flags de anomalia ao dicionário (in-place).

        Args:
            dados: Dicionário a ser modificado in-place.
        """
        anomalias = AnomalyDetector.detect_anomalies(dados)
        dados.update(anomalias)

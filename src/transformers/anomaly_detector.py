"""
Detector de anomalias estruturais em fundos FIDC.
"""
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detector de anomalias em dados de fundos"""
    
    @staticmethod
    def detect_anomalies(dados: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detecta anomalias estruturais nos dados.
        
        Args:
            dados: Dicionário com dados do fundo
            
        Returns:
            Dict com flags de anomalia
        """
        carteira_total = float(dados.get('CARTEIRA_TOTAL', 0) or 0)
        creditos_adquiridos = float(dados.get('CREDITOS_ADQUIRIDOS', 0) or 0)
        creditos_vencidos = float(dados.get('CRED_TOTAL_VENC_INADIMPL', 0) or 0)
        npl_ratio = float(dados.get('NPL_RATIO', 0) or 0)
        
        anomalias = {}
        
        # Flag 1: CREDITOS_ADQUIRIDOS muito baixo (< 1% da carteira)
        if carteira_total > 0:
            ratio_cred_carteira = (creditos_adquiridos / carteira_total) * 100
            anomalias['FLAG_CRED_ADQ_BAIXO'] = (ratio_cred_carteira < 1.0)
        else:
            anomalias['FLAG_CRED_ADQ_BAIXO'] = False
        
        # Flag 2: Créditos vencidos > 150% da carteira (anomalia contábil)
        if carteira_total > 0:
            anomalias['FLAG_VENCIDOS_ALTO'] = (creditos_vencidos > carteira_total * 1.5)
        else:
            anomalias['FLAG_VENCIDOS_ALTO'] = False
        
        # Flag 3: NPL implausível (> 500%)
        anomalias['FLAG_NPL_IMPLAUSIVEL'] = (npl_ratio > 500)
        
        # Flag 4: Carteira total zerada ou muito pequena
        anomalias['FLAG_CARTEIRA_ZERADA'] = (carteira_total < 1000)  # Menos de R$ 1.000
        
        # Flag consolidada: Anomalia estrutural
        anomalias['ANOMALIA_ESTRUTURAL'] = (
            anomalias['FLAG_CRED_ADQ_BAIXO'] or 
            anomalias['FLAG_VENCIDOS_ALTO'] or 
            anomalias['FLAG_NPL_IMPLAUSIVEL'] or
            anomalias['FLAG_CARTEIRA_ZERADA']
        )
        
        # Status de qualidade dos dados
        if anomalias['ANOMALIA_ESTRUTURAL']:
            if anomalias['FLAG_CARTEIRA_ZERADA']:
                anomalias['STATUS_DADOS'] = 'CARTEIRA_ZERADA'
            elif anomalias['FLAG_NPL_IMPLAUSIVEL']:
                anomalias['STATUS_DADOS'] = 'NPL_IMPLAUSIVEL'
            elif anomalias['FLAG_VENCIDOS_ALTO']:
                anomalias['STATUS_DADOS'] = 'ANOMALIA_CONTABIL'
            elif anomalias['FLAG_CRED_ADQ_BAIXO']:
                anomalias['STATUS_DADOS'] = 'ESTRUTURA_ATIPICA'
            else:
                anomalias['STATUS_DADOS'] = 'ANOMALIA_GENERICA'
        else:
            anomalias['STATUS_DADOS'] = 'VALIDADO'
        
        return anomalias
    
    @staticmethod
    def add_anomaly_flags(dados: Dict[str, Any]) -> None:
        """
        Detecta e adiciona flags de anomalia ao dicionário.
        
        Args:
            dados: Dicionário a ser modificado in-place
        """
        anomalias = AnomalyDetector.detect_anomalies(dados)
        dados.update(anomalias)

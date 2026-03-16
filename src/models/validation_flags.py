"""
Modelo de flags de validação para Quality Assurance (QA).

Contém as 5 flags de validação identificadas no processo de QA.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

from dataclasses import dataclass


@dataclass
class ValidationFlags:
    """
    Flags de validação de qualidade de dados FIDC.
    
    Attributes:
        ativo_zero_flag: True se ATIVO_TOTAL == 0
        diverge_liq_flag: True se há divergência entre liquidez calculada e informada
        carteira_bruta_zero_com_inad_flag: True se CARTEIRA_BRUTA == 0 e INADIMPLENCIA_TOTAL > 0
        diverge_npl_flag: True se há divergência entre NPL calculado e informado
        sem_posicao_flag: True se CARTEIRA_LIQUIDA_CALC == 0 e ATIVO_TOTAL > 0
    """
    
    ativo_zero_flag: bool = False
    diverge_liq_flag: bool = False
    carteira_bruta_zero_com_inad_flag: bool = False
    diverge_npl_flag: bool = False
    sem_posicao_flag: bool = False
    
    def has_issues(self) -> bool:
        """Retorna True se qualquer flag está ativa."""
        return any([
            self.ativo_zero_flag,
            self.diverge_liq_flag,
            self.carteira_bruta_zero_com_inad_flag,
            self.diverge_npl_flag,
            self.sem_posicao_flag,
        ])
    
    def to_dict(self) -> dict:
        """Converte para dicionário."""
        return {
            'ATIVO_ZERO_FLAG': self.ativo_zero_flag,
            'DIVERGE_LIQ_FLAG': self.diverge_liq_flag,
            'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG': self.carteira_bruta_zero_com_inad_flag,
            'DIVERGE_NPL_FLAG': self.diverge_npl_flag,
            'SEM_POSICAO_FLAG': self.sem_posicao_flag,
        }

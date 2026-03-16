"""
Modelo de dados para FIDC (Fundos de Investimento em Direitos Creditórios).

Contém todos os 90+ campos extraídos do XML do Informe Mensal, conforme notebook 'etl_fidic_vfinal.ipynb'.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FIDCData:
    """
    Representa todos os dados extraídos de um FIDC (90+ campos).
    
    Estrutura baseada no notebook 'etl_fidic_vfinal.ipynb'.
    """
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 1. IDENTIFICAÇÃO DO FUNDO
    # ═══════════════════════════════════════════════════════════════════════════
    cnpj_fundo: str = ""
    cnpj_administrador: str = ""
    data_competencia: str = ""  # Pode ser texto no XML original
    tipo_condominio: str = ""
    fundo_exclusivo: str = ""
    classe_unica: str = ""
    cotista_vinculado: str = ""
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 2. ATIVOS GERAIS
    # ═══════════════════════════════════════════════════════════════════════════
    ativo_total: float = 0.0
    disponibilidades: float = 0.0
    carteira_total: float = 0.0
    outros_ativos_total: float = 0.0
    outros_ativos_curto_prazo: float = 0.0
    outros_ativos_longo_prazo: float = 0.0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 3. CRÉDITOS EXISTENTES (Principal fonte de inadimplência)
    # ═══════════════════════════════════════════════════════════════════════════
    creditos_adquiridos: float = 0.0
    cred_vencidos_adimplentes: float = 0.0
    cred_vencidos_inadimplentes: float = 0.0
    cred_total_venc_inadimpl: float = 0.0
    cred_inadimplencia: float = 0.0
    cred_performados: float = 0.0
    cred_vencidos_pendentes: float = 0.0
    cred_emp_recuperacao: float = 0.0
    cred_receita_publica: float = 0.0
    cred_acao_judicial: float = 0.0
    cred_constituicao_juridica: float = 0.0
    cred_provisao_reducao: float = 0.0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 4. DIREITOS CREDITÓRIOS (DICRED)
    # ═══════════════════════════════════════════════════════════════════════════
    dicred_total: float = 0.0
    dicred_cedente: float = 0.0
    dicred_venc_inadimpl: float = 0.0
    dicred_total_venc_inad: float = 0.0
    dicred_inadimplencia: float = 0.0
    dicred_performados: float = 0.0
    dicred_venc_pendentes: float = 0.0
    dicred_emp_recuperacao: float = 0.0
    dicred_receita_publica: float = 0.0
    dicred_acao_judicial: float = 0.0
    dicred_provisao_reducao: float = 0.0

    # ═══════════════════════════════════════════════════════════════════════════
    # 5. VALORES MOBILIÁRIOS
    # ═══════════════════════════════════════════════════════════════════════════
    valores_mobiliarios_total: float = 0.0
    debentures: float = 0.0
    cri: float = 0.0
    notas_promissorias_comerciais: float = 0.0
    letras_financeiras: float = 0.0
    cotas_fif: float = 0.0
    outros_direitos_creditorios: float = 0.0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 6. OUTROS ATIVOS FINANCEIROS
    # ═══════════════════════════════════════════════════════════════════════════
    titulos_publicos_federais: float = 0.0
    cdb: float = 0.0
    aplicacoes_compromissadas: float = 0.0
    ativos_financeiros_rf: float = 0.0
    cotas_fidc: float = 0.0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 7. MERCADO DE DERIVATIVOS
    # ═══════════════════════════════════════════════════════════════════════════
    derivativos_total: float = 0.0
    termo_comprador: float = 0.0
    opcoes_titular: float = 0.0
    futuros_ajuste_positivo: float = 0.0
    swap_a_receber: float = 0.0
    cobertura_prestada: float = 0.0
    depositos_margem: float = 0.0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 8. SEGMENTAÇÃO DA CARTEIRA
    # ═══════════════════════════════════════════════════════════════════════════
    carteira_segmentada_total: float = 0.0
    
    # Setores Gerais
    segmt_industrial: float = 0.0
    segmt_mercado_imobiliario: float = 0.0
    segmt_agronegocio: float = 0.0
    segmt_cartao_credito: float = 0.0
    segmt_acao_judicial: float = 0.0
    segmt_propriedade_intelectual: float = 0.0
    
    # Comercial
    segmt_comercial_total: float = 0.0
    segmt_comercio: float = 0.0
    segmt_comercio_varejo: float = 0.0
    segmt_arrend_mercantil: float = 0.0

    # Serviços
    segmt_servicos_total: float = 0.0
    segmt_servicos_gerais: float = 0.0
    segmt_servicos_publicos: float = 0.0
    segmt_servicos_educacao: float = 0.0
    segmt_servicos_entretenimento: float = 0.0
    
    # Financeiro
    segmt_financeiro_total: float = 0.0
    segmt_financ_credito_pessoa: float = 0.0
    segmt_financ_consignado: float = 0.0
    segmt_financ_corporativo: float = 0.0
    segmt_financ_middle_market: float = 0.0
    segmt_financ_veiculos: float = 0.0
    segmt_financ_imob_empresarial: float = 0.0
    segmt_financ_imob_residencial: float = 0.0
    segmt_financ_outros: float = 0.0

    # Factoring
    segmt_factoring_total: float = 0.0
    segmt_factoring_pessoa: float = 0.0
    segmt_factoring_corporativo: float = 0.0
    
    # Setor Público
    segmt_setor_publico_total: float = 0.0
    segmt_precatorios: float = 0.0
    segmt_creditos_tributarios: float = 0.0
    segmt_royalties: float = 0.0
    segmt_setor_publico_outros: float = 0.0
    
    # ═══════════════════════════════════════════════════════════════════════════
    # 9. INDICADORES E METADADOS
    # ═══════════════════════════════════════════════════════════════════════════
    inadimplencia_total: float = 0.0
    indice_npl_decimal: float = 0.0  # Mantido 0-1 para consistência interna
    indice_npl_percentual: float = 0.0 # Campo do notebook (0-100)
    
    taxa_liquidez_percentual: float = 0.0
    concentracao_credito_percentual: float = 0.0
    
    # Metadados
    status: str = "PENDENTE"
    data_referencia_doc: str = ""
    id_documento: str = ""
    mensagem_erro: Optional[str] = None

    def to_dict(self) -> dict:
        """Converte o dataclass para dicionário (flat)."""
        return {k.upper(): v for k, v in self.__dict__.items()}

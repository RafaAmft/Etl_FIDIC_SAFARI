"""
Parser XML para Informes Mensais de FIDC.

Implementação "Golden Source" baseada no notebook 'etl_fidic_vfinal.ipynb'.
Extrai 90+ campos com regras específicas de negócio.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import xml.etree.ElementTree as ET
import logging
from typing import Tuple, Optional, Any

from ..models.fidc_data import FIDCData

logger = logging.getLogger(__name__)


def limpar_tag(tag: str) -> str:
    """Remove namespace XML das tags."""
    return tag.split('}')[-1] if '}' in tag else tag


def converter_valor(texto: str) -> float:
    """
    Converte string no formato brasileiro para float.
    Ex: "1.234.567,89" -> 1234567.89
    """
    if not texto or not str(texto).strip():
        return 0.0
    try:
        texto_limpo = str(texto).replace('.', '').replace(',', '.')
        return float(texto_limpo)
    except (ValueError, AttributeError):
        return 0.0


def buscar_valor_xml(root: ET.Element, caminho: str) -> Any:
    """
    Busca um valor no XML e tenta converter para float.
    Se não conseguir, retorna como string.
    
    Lógica idêntica ao notebook 'etl_fidic_vfinal.ipynb', mas com proteção 
    explícita para campos de data e identificação que não devem ser floats.
    """
    elemento = root.find(f'.//{caminho}')

    if elemento is not None and elemento.text:
        texto = elemento.text.strip()
        
        # ✅ Campos que devem SEMPRE ser string (Forçar retorno de texto)
        # DT_COMPT, NR_CNPJ, CD_, etc.
        if any(x in caminho for x in ['DT_', 'NR_', 'CD_', 'FDO_', 'CLASS_', 'COTST_', 'TP_']):
            return texto
            
        try:
            return converter_valor(elemento.text)
        except:
            return texto
    
    # Retornar string vazia para campos de texto conhecidos se não encontrar
    if any(x in caminho for x in ['DT_', 'NR_', 'CD_', 'FDO_', 'CLASS_', 'COTST_', 'TP_']):
        return ""
    
    # Retornar 0.0 para campos numéricos (Heurística por prefixo ou caminho)
    # Se tem '/' (caminho composto) ou começa com VL_/QT_/PR_/TX_ -> é número
    if '/' in caminho or any(caminho.startswith(x) for x in ['VL_', 'QT_', 'PR_', 'TX_']):
        return 0.0
        
    return ""


class FIDCXMLParser:
    """Parser completo para 90+ campos de FIDC."""

    def parse(self, xml_content: bytes, cnpj_fallback: str = "") -> Tuple[bool, Optional[FIDCData], Optional[str]]:
        try:
            root = ET.fromstring(xml_content)
        except Exception as e:
            logger.error(f"Erro ao parsear XML: {e}")
            return False, None, f"Erro XML: {str(e)}"

        try:
            dados = FIDCData()
            
            # ════════════════════════════════════════════════════════════════
            # 1. IDENTIFICAÇÃO
            # ════════════════════════════════════════════════════════════════
            dados.cnpj_fundo = str(buscar_valor_xml(root, 'NR_CNPJ_FUNDO')) or cnpj_fallback
            dados.cnpj_administrador = str(buscar_valor_xml(root, 'NR_CNPJ_ADM'))
            dados.data_competencia = str(buscar_valor_xml(root, 'DT_COMPT'))
            dados.tipo_condominio = str(buscar_valor_xml(root, 'TP_CONDOMINIO'))
            dados.fundo_exclusivo = str(buscar_valor_xml(root, 'FDO_EXCL'))
            dados.classe_unica = str(buscar_valor_xml(root, 'CLASS_UNICA'))
            dados.cotista_vinculado = str(buscar_valor_xml(root, 'COTST_VINCUL'))

            # ════════════════════════════════════════════════════════════════
            # 2. ATIVOS GERAIS
            # ════════════════════════════════════════════════════════════════
            dados.ativo_total = buscar_valor_xml(root, 'VL_SOM_APLIC_ATIVO')
            dados.disponibilidades = buscar_valor_xml(root, 'VL_DISPONIB')
            dados.carteira_total = buscar_valor_xml(root, 'VL_CARTEIRA')
            dados.outros_ativos_total = buscar_valor_xml(root, 'VL_SOM_OUTROS_ATIVOS')
            dados.outros_ativos_curto_prazo = buscar_valor_xml(root, 'OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_CURPRZ')
            dados.outros_ativos_longo_prazo = buscar_valor_xml(root, 'OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_LPRAZO')

            # ════════════════════════════════════════════════════════════════
            # 3. CRÉDITOS EXISTENTES
            # ════════════════════════════════════════════════════════════════
            dados.creditos_adquiridos = buscar_valor_xml(root, 'CRED_EXISTE/VL_SOM_DICRED_AQUIS')
            dados.cred_vencidos_adimplentes = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_EXISTE_VENC_ADIMPL')
            dados.cred_vencidos_inadimplentes = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_EXISTE_VENC_INAD')
            dados.cred_total_venc_inadimpl = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_TOTAL_VENC_INAD')
            dados.cred_inadimplencia = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_EXISTE_INAD')
            dados.cred_performados = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_REFER_DICRED_PERFO')
            dados.cred_vencidos_pendentes = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_VENC_PEND')
            dados.cred_emp_recuperacao = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_ORIGEM_EMP_PROC_RECUP')
            dados.cred_receita_publica = buscar_valor_xml(root, 'CRED_EXISTE/VL_DECOR_RECEIT_PUBLIC')
            dados.cred_acao_judicial = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_ACAO_JUDIC')
            dados.cred_constituicao_juridica = buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_CONST_JUR_FATRISC')
            dados.cred_provisao_reducao = buscar_valor_xml(root, 'CRED_EXISTE/VL_PROVIS_REDUC_RECUP')

            # ════════════════════════════════════════════════════════════════
            # 4. DIREITOS CREDITÓRIOS (DICRED)
            # ════════════════════════════════════════════════════════════════
            dados.dicred_total = buscar_valor_xml(root, 'DICRED/VL_DICRED')
            dados.dicred_cedente = buscar_valor_xml(root, 'DICRED/VL_DICRED_CEDENT')
            dados.dicred_venc_inadimpl = buscar_valor_xml(root, 'DICRED/VL_DICRED_EXISTE_VENC_INAD')
            dados.dicred_total_venc_inad = buscar_valor_xml(root, 'DICRED/VL_DICRED_TOTAL_VENC_INAD')
            dados.dicred_inadimplencia = buscar_valor_xml(root, 'DICRED/VL_DICRED_EXISTE_INAD')
            dados.dicred_performados = buscar_valor_xml(root, 'DICRED/VL_DICRED_REFER_DICRED_PERFO')
            dados.dicred_venc_pendentes = buscar_valor_xml(root, 'DICRED/VL_DICRED_VENC_PEND')
            dados.dicred_emp_recuperacao = buscar_valor_xml(root, 'DICRED/VL_DICRED_ORIGEM_EMP_PROC_RECUP')
            dados.dicred_receita_publica = buscar_valor_xml(root, 'DICRED/VL_DICRED_RECEIT_PUBLIC')
            dados.dicred_acao_judicial = buscar_valor_xml(root, 'DICRED/VL_DICRED_ACAO_JUDIC')
            dados.dicred_provisao_reducao = buscar_valor_xml(root, 'DICRED/VL_DICRED_PROVIS_REDUC_RECUP')

            # ════════════════════════════════════════════════════════════════
            # 5. VALORES MOBILIÁRIOS
            # ════════════════════════════════════════════════════════════════
            dados.valores_mobiliarios_total = buscar_valor_xml(root, 'VALORES_MOB/VL_SOM_VALORES_MOB')
            dados.debentures = buscar_valor_xml(root, 'VALORES_MOB/VL_DEBT')
            dados.cri = buscar_valor_xml(root, 'VALORES_MOB/VL_CRI')
            dados.notas_promissorias_comerciais = buscar_valor_xml(root, 'VALORES_MOB/VL_NP_COMERC')
            dados.letras_financeiras = buscar_valor_xml(root, 'VALORES_MOB/VL_LETRA_FINANC')
            dados.cotas_fif = buscar_valor_xml(root, 'VALORES_MOB/VL_CLS_COTA_FIF')
            dados.outros_direitos_creditorios = buscar_valor_xml(root, 'VALORES_MOB/VL_OUTRO_DICRED')

            # ════════════════════════════════════════════════════════════════
            # 6. OUTROS ATIVOS FINANCEIROS
            # ════════════════════════════════════════════════════════════════
            dados.titulos_publicos_federais = buscar_valor_xml(root, 'VL_TITPUB_FED')
            dados.cdb = buscar_valor_xml(root, 'VL_CDB')
            dados.aplicacoes_compromissadas = buscar_valor_xml(root, 'VL_APLIC_OPER_COMPSS')
            dados.ativos_financeiros_rf = buscar_valor_xml(root, 'VL_ATIV_FINANC_RF')
            dados.cotas_fidc = buscar_valor_xml(root, 'VL_COTA_FIDC')

            # ════════════════════════════════════════════════════════════════
            # 7. MERCADO DE DERIVATIVOS
            # ════════════════════════════════════════════════════════════════
            dados.derivativos_total = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_SOM_MERC_DERIVATIVO')
            dados.termo_comprador = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_TERMO_POS_COMPRD')
            dados.opcoes_titular = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_OP_POS_TITUL')
            dados.futuros_ajuste_positivo = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_FUT_AJUST_POSIT')
            dados.swap_a_receber = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_DIFER_SWAP_RECEB')
            dados.cobertura_prestada = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_COBERT_PREST')
            dados.depositos_margem = buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_DEPOS_MARGEM')

            # ════════════════════════════════════════════════════════════════
            # 8. SEGMENTAÇÃO
            # ════════════════════════════════════════════════════════════════
            dados.carteira_segmentada_total = buscar_valor_xml(root, 'CART_SEGMT/VL_SOM_CART_SEGMT')
            
            dados.segmt_industrial = buscar_valor_xml(root, 'CART_SEGMT/VL_IND')
            dados.segmt_mercado_imobiliario = buscar_valor_xml(root, 'CART_SEGMT/VL_MERC_IMOBIL')
            dados.segmt_agronegocio = buscar_valor_xml(root, 'CART_SEGMT/VL_AGRONEG')
            dados.segmt_cartao_credito = buscar_valor_xml(root, 'CART_SEGMT/VL_CART_CRED')
            dados.segmt_acao_judicial = buscar_valor_xml(root, 'CART_SEGMT/VL_ACAO_JUDIC')
            dados.segmt_propriedade_intelectual = buscar_valor_xml(root, 'CART_SEGMT/VL_PROPRD_MARCA_PATENT')

            # Comercial
            dados.segmt_comercial_total = buscar_valor_xml(root, 'SEGMT_COMERC/VL_SOM_SEGMT_COMERC')
            dados.segmt_comercio = buscar_valor_xml(root, 'SEGMT_COMERC/VL_COMERC')
            dados.segmt_comercio_varejo = buscar_valor_xml(root, 'SEGMT_COMERC/VL_COMERC_VARJ')
            dados.segmt_arrend_mercantil = buscar_valor_xml(root, 'SEGMT_COMERC/VL_ARREND_MERCNT')

            # Serviços
            dados.segmt_servicos_total = buscar_valor_xml(root, 'SEGMT_SERV/VL_SOM_SEGMT_SERV')
            dados.segmt_servicos_gerais = buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV')
            dados.segmt_servicos_publicos = buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_PUBLIC')
            dados.segmt_servicos_educacao = buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_EDUC')
            dados.segmt_servicos_entretenimento = buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_ENTRETEN')

            # Financeiro
            dados.segmt_financeiro_total = buscar_valor_xml(root, 'SEGMT_FINANC/VL_SOM_SEGMT_FINANC')
            dados.segmt_financ_credito_pessoa = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_PESSOA')
            dados.segmt_financ_consignado = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_PESSOA_CONSIG')
            dados.segmt_financ_corporativo = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_CORPOR')
            dados.segmt_financ_middle_market = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_MMARKET')
            dados.segmt_financ_veiculos = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_VEICL')
            dados.segmt_financ_imob_empresarial = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_IMOBIL_EMPSRL')
            dados.segmt_financ_imob_residencial = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_IMOBIL_RESID')
            dados.segmt_financ_outros = buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_OUTRO')

            # Factoring
            dados.segmt_factoring_total = buscar_valor_xml(root, 'SEGMT_FACT/VL_SOM_SEGMT_FACT')
            dados.segmt_factoring_pessoa = buscar_valor_xml(root, 'SEGMT_FACT/VL_FACT_PESSOA')
            dados.segmt_factoring_corporativo = buscar_valor_xml(root, 'SEGMT_FACT/VL_FACT_CORPOR')

            # Setor Público
            dados.segmt_setor_publico_total = buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SOM_SEGMT_SETOR_PUBLIC')
            dados.segmt_precatorios = buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_PRECAT')
            dados.segmt_creditos_tributarios = buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_CRED_TRIBUT')
            dados.segmt_royalties = buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_ROYA')
            dados.segmt_setor_publico_outros = buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_OUTRO')

            # ════════════════════════════════════════════════════════════════
            # 9. CÁLCULO DE INDICADORES (CONFORME NOTEBOOK)
            # ════════════════════════════════════════════════════════════════

            # Inadimplência consolidada (prioriza maior valor entre CRED e DICRED)
            # Notebook line: dados['INADIMPLENCIA_TOTAL'] = max(inadimpl_cred, inadimpl_dicred)
            inadimpl_cred = float(dados.cred_inadimplencia or 0)
            inadimpl_dicred = float(dados.dicred_inadimplencia or 0)
            dados.inadimplencia_total = max(inadimpl_cred, inadimpl_dicred)

            # Carteira de crédito para cálculo de NPL (CREDITOS_ADQUIRIDOS)
            carteira_credito = float(dados.creditos_adquiridos or 0)
            dados.carteira_bruta = carteira_credito # Alias para compatibilidade

            # Índice de NPL (Non-Performing Loans)
            if carteira_credito > 0 and dados.inadimplencia_total > 0:
                npl_percent = (dados.inadimplencia_total / carteira_credito) * 100
                dados.indice_npl_decimal = min(npl_percent / 100.0, 1.0) # 0-1
                dados.indice_npl_percentual = npl_percent # 0-100 (Notebook format)
            else:
                dados.indice_npl_decimal = 0.0
                dados.indice_npl_percentual = 0.0

            # Taxa de liquidez imediata (Disponibilidades / Ativo Total)
            ativo_total = float(dados.ativo_total or 0)
            disponibilidades = float(dados.disponibilidades or 0)
            
            if ativo_total > 0:
                dados.taxa_liquidez_percentual = (disponibilidades / ativo_total) * 100
                dados.taxa_liquidez_decimal = dados.taxa_liquidez_percentual / 100.0
            else:
                dados.taxa_liquidez_percentual = 0.0
                dados.taxa_liquidez_decimal = 0.0

            # Concentração em crédito (Carteira / Ativo Total)
            if ativo_total > 0:
                dados.concentracao_credito_percentual = (carteira_credito / ativo_total) * 100
                dados.concentracao_credito_decimal = dados.concentracao_credito_percentual / 100.0
            else:
                dados.concentracao_credito_percentual = 0.0
                dados.concentracao_credito_decimal = 0.0
                
            dados.status = "SUCESSO"
            return True, dados, None

        except Exception as e:
            logger.error(f"Erro ao extrair dados logica notebook: {e}")
            return False, None, f"Erro Extração: {str(e)}"

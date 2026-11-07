"""
Extrator de campos do XML de FIDC.
Extrai todos os 100+ campos relevantes para análise de fundos distressed.
"""
import logging
import json
import xml.etree.ElementTree as ET
from typing import Dict, Any, List
from src.extractors.xml_parser import XMLParser
from src.extractors.date_converter import DateConverter

logger = logging.getLogger(__name__)


class FieldExtractor:
    """Extrator de campos XML"""
    
    def __init__(self):
        self.parser = XMLParser()
        self.date_converter = DateConverter()
    
    def extract_all_fields(self, xml_content: bytes) -> Dict[str, Any]:
        """
        Extrai TODOS os campos relevantes do XML.
        
        Args:
            xml_content: Conteúdo XML em bytes
            
        Returns:
            Dicionário com 100+ campos estruturados
        """
        root = self.parser.parse_xml(xml_content)
        if root is None:
            raise ValueError("Não foi possível fazer parse do XML")
        
        # Extrair todos os campos básicos
        dados = self._extract_basic_fields(root)
        
        # Converter data de competência
        dados['DATA_COMPETENCIA'] = self._extract_and_convert_date(root, dados)
        
        # Extrair cedentes concentrados
        self._extract_cedentes_concentrados(root, dados)
        
        return dados
    
    def _extract_basic_fields(self, root: ET.Element) -> Dict[str, Any]:
        """Extrai campos básicos do XML"""
        buscar = self.parser.buscar_valor_xml
        
        return {
            # IDENTIFICAÇÃO DO FUNDO
            'CNPJ_FUNDO': buscar(root, 'NR_CNPJ_FUNDO'),
            'CNPJ_ADMINISTRADOR': buscar(root, 'NR_CNPJ_ADM'),
            'DATA_COMPETENCIA': buscar(root, 'DT_COMPT'),
            'TIPO_CONDOMINIO': buscar(root, 'TP_CONDOMINIO'),
            'FUNDO_EXCLUSIVO': buscar(root, 'FDO_EXCL'),
            'CLASSE_UNICA': buscar(root, 'CLASS_UNICA'),
            'COTISTA_VINCULADO': buscar(root, 'COTST_VINCUL'),
            
            # ATIVOS GERAIS
            'ATIVO_TOTAL': buscar(root, 'APLIC_ATIVO/VL_SOM_APLIC_ATIVO'),
            'DISPONIBILIDADES': buscar(root, 'APLIC_ATIVO/VL_DISPONIB'),
            'CARTEIRA_TOTAL': buscar(root, 'APLIC_ATIVO/VL_CARTEIRA'),
            'OUTROS_ATIVOS_TOTAL': buscar(root, 'APLIC_ATIVO/OUTROS_ATIVOS/VL_SOM_OUTROS_ATIVOS'),
            'OUTROS_ATIVOS_CURTO_PRAZO': buscar(root, 'APLIC_ATIVO/OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_CURPRZ'),
            'OUTROS_ATIVOS_LONGO_PRAZO': buscar(root, 'APLIC_ATIVO/OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_LPRAZO'),
            
            # PASSIVO E PATRIMÔNIO LÍQUIDO
            'PASSIVO_CIRCULANTE': buscar(root, 'PASSIV/PASSIV_VALORES/VL_PGTO_CURPRZ'),
            'PASSIVO_EXIGIVEL_LONGO_PRAZO': buscar(root, 'PASSIV/PASSIV_VALORES/VL_PGTO_LPRAZO'),
            'PATRIMONIO_LIQUIDO': buscar(root, 'PATRLIQ/VL_PATRIM_LIQ'),
            
            # CRÉDITOS EXISTENTES
            'CREDITOS_ADQUIRIDOS': buscar(root, 'CRED_EXISTE/VL_SOM_DICRED_AQUIS'),
            'CRED_VENCIDOS_ADIMPLENTES': buscar(root, 'CRED_EXISTE/VL_CRED_EXISTE_VENC_ADIMPL'),
            'CRED_VENCIDOS_INADIMPLENTES': buscar(root, 'CRED_EXISTE/VL_CRED_EXISTE_VENC_INAD'),
            'CRED_TOTAL_VENC_INADIMPL': buscar(root, 'CRED_EXISTE/VL_CRED_TOTAL_VENC_INAD'),
            'CRED_INADIMPLENCIA': buscar(root, 'CRED_EXISTE/VL_CRED_EXISTE_INAD'),
            'CRED_PERFORMADOS': buscar(root, 'CRED_EXISTE/VL_CRED_REFER_DICRED_PERFO'),
            'CRED_VENCIDOS_PENDENTES': buscar(root, 'CRED_EXISTE/VL_CRED_VENC_PEND'),
            'CRED_EMP_RECUPERACAO': buscar(root, 'CRED_EXISTE/VL_CRED_ORIGEM_EMP_PROC_RECUP'),
            'CRED_RECEITA_PUBLICA': buscar(root, 'CRED_EXISTE/VL_DECOR_RECEIT_PUBLIC'),
            'CRED_ACAO_JUDICIAL': buscar(root, 'CRED_EXISTE/VL_CRED_ACAO_JUDIC'),
            'CRED_CONSTITUICAO_JURIDICA': buscar(root, 'CRED_EXISTE/VL_CRED_CONST_JUR_FATRISC'),
            'CRED_PROVISAO_REDUCAO': buscar(root, 'CRED_EXISTE/VL_PROVIS_REDUC_RECUP'),
            
            # AGING DA CARTEIRA
            'AGING_A_VENCER': buscar(root, 'COMPMT_DICRED_AQUIS/VL_SOM_PRAZO_VENC'),
            'AGING_VENC_1_30_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_30'),
            'AGING_VENC_31_60_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_31_60'),
            'AGING_VENC_61_90_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_61_90'),
            'AGING_VENC_91_120_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_91_120'),
            'AGING_VENC_121_150_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_121_150'),
            'AGING_VENC_151_180_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_151_180'),
            'AGING_VENC_181_360_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_181_360'),
            'AGING_VENC_361_720_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_361_720'),
            'AGING_VENC_721_1080_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_721_1080'),
            'AGING_VENC_MAIOR_1080_DIAS': buscar(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_1080'),
            
            # DIREITOS CREDITÓRIOS (DICRED)
            'DICRED_TOTAL': buscar(root, 'DICRED/VL_DICRED'),
            'DICRED_CEDENTE': buscar(root, 'DICRED/VL_DICRED_CEDENT'),
            'DICRED_VENC_INADIMPL': buscar(root, 'DICRED/VL_DICRED_EXISTE_VENC_INAD'),
            'DICRED_TOTAL_VENC_INAD': buscar(root, 'DICRED/VL_DICRED_TOTAL_VENC_INAD'),
            'DICRED_INADIMPLENCIA': buscar(root, 'DICRED/VL_DICRED_EXISTE_INAD'),
            'DICRED_PERFORMADOS': buscar(root, 'DICRED/VL_DICRED_REFER_DICRED_PERFO'),
            'DICRED_VENC_PENDENTES': buscar(root, 'DICRED/VL_DICRED_VENC_PEND'),
            'DICRED_EMP_RECUPERACAO': buscar(root, 'DICRED/VL_DICRED_ORIGEM_EMP_PROC_RECUP'),
            'DICRED_RECEITA_PUBLICA': buscar(root, 'DICRED/VL_DICRED_RECEIT_PUBLIC'),
            'DICRED_ACAO_JUDICIAL': buscar(root, 'DICRED/VL_DICRED_ACAO_JUDIC'),
            'DICRED_PROVISAO_REDUCAO': buscar(root, 'DICRED/VL_DICRED_PROVIS_REDUC_RECUP'),
            
            # MOVIMENTAÇÃO DA SUBORDINADA
            'SUBORD_CAPTACOES': buscar(root, 'CAPTA_RESGA_AMORTI/CAPT_MES/CLASSE_SUBORD/VL_COTAS'),
            'SUBORD_AMORTIZACOES': buscar(root, 'CAPTA_RESGA_AMORTI/AMORT/CLASSE_SUBORD/VL_TOTAL'),
            'SUBORD_RESGATES': buscar(root, 'CAPTA_RESGA_AMORTI/RESG_MES/CLASSE_SUBORD/VL_COTAS'),
            
            # VALORES MOBILIÁRIOS
            'VALORES_MOBILIARIOS_TOTAL': buscar(root, 'VALORES_MOB/VL_SOM_VALORES_MOB'),
            'DEBENTURES': buscar(root, 'VALORES_MOB/VL_DEBT'),
            'CRI': buscar(root, 'VALORES_MOB/VL_CRI'),
            'NOTAS_PROMISSORIAS_COMERCIAIS': buscar(root, 'VALORES_MOB/VL_NP_COMERC'),
            'LETRAS_FINANCEIRAS': buscar(root, 'VALORES_MOB/VL_LETRA_FINANC'),
            'COTAS_FIF': buscar(root, 'VALORES_MOB/VL_COTA_FDO_ICVM409'),
            'OUTROS_DIREITOS_CREDITORIOS': buscar(root, 'VALORES_MOB/VL_OUTRO_DICRED'),
            
            # OUTROS ATIVOS FINANCEIROS
            'TITULOS_PUBLICOS_FEDERAIS': buscar(root, 'VL_TITPUB_FED'),
            'CDB': buscar(root, 'VL_CDB'),
            'APLICACOES_COMPROMISSADAS': buscar(root, 'VL_APLIC_OPER_COMPSS'),
            'ATIVOS_FINANCEIROS_RF': buscar(root, 'VL_ATIV_FINANC_RF'),
            'COTAS_FIDC': buscar(root, 'VL_COTA_FIDC')
        }
    
    def _extract_and_convert_date(self, root: ET.Element, dados: Dict[str, Any]) -> str:
        """Extrai e converte data de competência"""
        data_original = dados.get('DATA_COMPETENCIA')
        
        # Tentar converter data original
        data_convertida = self.date_converter.convert_to_iso(data_original)
        
        # Se falhou, tentar fallback
        if not data_convertida:
            data_ref = self.parser.buscar_valor_xml(root, 'DT_COMPTC')
            data_convertida = self.date_converter.convert_to_iso(data_ref)
        
        return data_convertida
    
    def _extract_cedentes_concentrados(self, root: ET.Element, dados: Dict[str, Any]) -> None:
        """Extrai cedentes concentrados (> 10% PL)"""
        try:
            patrimonio_liquido = float(dados.get('PATRIMONIO_LIQUIDO', 0) or 0)
            cedentes_list = []
            
            cedentes_nodes = root.findall('.//CEDENTE')
            for cedente_node in cedentes_nodes:
                try:
                    cnpj_cedente = self.parser.buscar_valor_xml(cedente_node, 'NR_CNPJ_CEDENT')
                    nome_cedente = self.parser.buscar_valor_xml(cedente_node, 'RAZAO_SOCIAL_CEDENT')
                    valor_cedente = float(self.parser.buscar_valor_xml(cedente_node, 'VL_DICRED_CEDENT') or 0)
                    
                    if patrimonio_liquido > 0 and valor_cedente > 0:
                        perc_pl = (valor_cedente / patrimonio_liquido) * 100
                        if perc_pl > 10:
                            cedentes_list.append({
                                'CNPJ': str(cnpj_cedente),
                                'Nome': str(nome_cedente),
                                'Valor': float(valor_cedente),
                                'Perc_PL': round(perc_pl, 2)
                            })
                except Exception as e:
                    logger.debug(f"Erro ao extrair cedente individual: {e}")
                    continue
            
            if cedentes_list:
                dados['CEDENTES_CONCENTRADOS_JSON'] = json.dumps(cedentes_list, ensure_ascii=False)
                dados['QTD_CEDENTES_CONCENTRADOS'] = len(cedentes_list)
            else:
                dados['CEDENTES_CONCENTRADOS_JSON'] = ''
                dados['QTD_CEDENTES_CONCENTRADOS'] = 0
        except Exception as e:
            logger.warning(f"Erro ao processar cedentes concentrados: {e}")
            dados['CEDENTES_CONCENTRADOS_JSON'] = ''
            dados['QTD_CEDENTES_CONCENTRADOS'] = 0

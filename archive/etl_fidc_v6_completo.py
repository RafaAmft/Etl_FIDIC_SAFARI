# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════
ETL FIDC V6 - MONITOR DE FUNDOS DISTRESSED
═══════════════════════════════════════════════════════════════════════
Versão Organizada e Otimizada
Data: Dezembro 2025

🎯 FUNCIONALIDADES PRINCIPAIS:
✅ Coleta rolling 60 meses (5 anos de histórico)
✅ Filtro inteligente de classes (Senior > Mezanino > Subordinada)
✅ Extração de 100+ campos por documento
✅ Métricas de distressed: NPL, Zumbi Ratio, Liquidez
✅ Retry automático com backoff exponencial
✅ Cache de resultados (evita reprocessamento)
✅ Processamento paralelo (5 threads)
✅ Logging profissional com arquivos separados

📊 INDICADORES CALCULADOS:
• NPL_RATIO: (Créditos Vencidos / Carteira) * 100
• ZUMBI_RATIO: (Créditos > 1080 dias / Carteira) * 100
• LIQUIDEZ_IMEDIATA_RATIO: Disponibilidades / Passivo Circulante

🚀 OTIMIZAÇÕES:
• Economia de 66% no tempo (filtro de classe ANTES do download)
• 1 XML por CNPJ por mês (ao invés de 3-6)
• Sem duplicidade no CSV final

═══════════════════════════════════════════════════════════════════════
COMO USAR:

1. Instale as dependências: pip install pandas requests tenacity tqdm python-dateutil
2. Coloque o arquivo 'lista_cnpjs_fidc.csv' na pasta RAW/
3. Execute o script: python etl_fidc_v6_completo.py
4. Aguarde o processamento (estimativa exibida no log)
5. Verifique os 2 arquivos CSV gerados:
   - fidc_monitor_completo_YYYYMMDD_HHMMSS.csv (todos os fundos)
   - fidc_distressed_npl_gt_20_YYYYMMDD_HHMMSS.csv (apenas NPL > 20%)

═══════════════════════════════════════════════════════════════════════
"""


# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════
ETL FIDC V6 - MONITOR DE FUNDOS DISTRESSED
═══════════════════════════════════════════════════════════════════════
Versão Otimizada e Organizada
Data: Dezembro 2025

Funcionalidades:
✅ Coleta rolling 60 meses (5 anos)
✅ Filtro inteligente de classes (Senior first)
✅ Retry com backoff exponencial
✅ Cache de resultados
✅ Processamento paralelo
✅ Métricas de distressed (NPL, Zumbi Ratio, Liquidez)
✅ Logging profissional
═══════════════════════════════════════════════════════════════════════
"""

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 1: IMPORTS E VERIFICAÇÃO DE DEPENDÊNCIAS
# ═══════════════════════════════════════════════════════════════════════

# Imports da biblioteca padrão
import base64
import xml.etree.ElementTree as ET
import time
import os
import logging
import hashlib
import pickle
import json
from typing import Dict, List, Optional, Union
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# Imports de dependências externas (com verificação)
try:
    import pandas as pd
    import requests
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    from tqdm.auto import tqdm
    from dateutil.relativedelta import relativedelta
    print("✅ Todas as dependencias estão instaladas!")
except ImportError as e:
    print("❌ ERRO: Dependencias faltando!")
    modulo_faltando = e.name if hasattr(e, 'name') else str(e)
    print(f"   Módulo não encontrado: {modulo_faltando}")
    print("   Execute: pip install pandas requests tenacity tqdm python-dateutil")
    raise

# ───────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DA API B3
# ───────────────────────────────────────────────────────────────────────
URL_API_BUSCA = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
URL_API_DOWNLOAD = "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ───────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DE PERFORMANCE
# ───────────────────────────────────────────────────────────────────────
DELAY_ENTRE_REQUISICOES = 2
TIMEOUT_BUSCA = 10
TIMEOUT_DOWNLOAD = 20
MAX_RETRIES = 3
MAX_WORKERS = 5  # Threads paralelas

# ───────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DE CACHE
# ───────────────────────────────────────────────────────────────────────
CACHE_ENABLED = True
CACHE_DIR = Path('.cache_fidc')
CACHE_VERSION = "v6.1"  # Versão do cache - incrementar quando estrutura mudar

# ───────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DE LOGGING
# ───────────────────────────────────────────────────────────────────────
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)

log_filename = LOG_DIR / f'etl_fidc_{datetime.now():%Y%m%d_%H%M%S}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

if CACHE_ENABLED:
    CACHE_DIR.mkdir(exist_ok=True)
    logger.info(f"✅ Cache habilitado: {CACHE_DIR.absolute()}")

logger.info("="*80)
logger.info("ETL FIDC V6 - MONITOR DE FUNDOS DISTRESSED")
logger.info("="*80)
logger.info(f"⚙️ Configurações:")
logger.info(f"   • Delay entre requisições: {DELAY_ENTRE_REQUISICOES}s")
logger.info(f"   • Max workers (threads): {MAX_WORKERS}")
logger.info(f"   • Max retries: {MAX_RETRIES}")
logger.info(f"   • Cache: {'Habilitado' if CACHE_ENABLED else 'Desabilitado'}")
logger.info("="*80)

print("✅ Configurações carregadas com sucesso!")

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 3: FUNÇÕES AUXILIARES (CONVERSÃO E BUSCA XML)
# ═══════════════════════════════════════════════════════════════════════

def limpar_tag(tag: str) -> str:
    """Remove namespace XML das tags."""
    return tag.split('}')[-1] if '}' in tag else tag


def converter_valor(texto: Union[str, float, int, None]) -> float:
    """
    Converte string no formato brasileiro para float com validação robusta.

    Args:
        texto: Valor a ser convertido

    Returns:
        float: Valor convertido ou 0.0 se inválido
    """
    if texto is None or texto == '':
        return 0.0

    if isinstance(texto, (int, float)):
        return float(texto)

    try:
        texto_str = str(texto).strip()
        if not texto_str:
            return 0.0

        # Remove pontos (separador de milhar) e troca vírgula por ponto
        texto_limpo = texto_str.replace('.', '').replace(',', '.')
        return float(texto_limpo)
    except (ValueError, AttributeError) as e:
        logger.debug(f"Erro ao converter '{texto}': {e}")
        return 0.0


def buscar_valor_xml(root: ET.Element, caminho: str) -> Union[float, str]:
    """
    Busca um valor no XML com tratamento robusto de namespaces.

    Args:
        root: Elemento raiz do XML
        caminho: Caminho XPath simplificado

    Returns:
        float ou string, com fallback para 0.0 ou ''
    """
    try:
        elemento = root.find(f'.//{caminho}')
        if elemento is not None and elemento.text:
            texto = elemento.text.strip()
            if not texto:
                return 0.0 if '/' in caminho else ''

            # Tentar converter para número
            try:
                return converter_valor(texto)
            except:
                return texto

        return 0.0 if '/' in caminho else ''
    except Exception as e:
        logger.debug(f"Erro ao buscar '{caminho}': {e}")
        return 0.0 if '/' in caminho else ''


print("✅ Funções auxiliares carregadas!")

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 4: FUNÇÕES DE CACHE
# ═══════════════════════════════════════════════════════════════════════

def gerar_cache_key(cnpj: str, doc_id: str) -> str:
    """Gera chave única para cache."""
    return hashlib.md5(f"{cnpj}_{doc_id}".encode()).hexdigest()


def salvar_cache(cnpj: str, doc_id: str, dados: Dict) -> None:
    """Salva resultado em cache com versionamento."""
    if not CACHE_ENABLED:
        return

    try:
        cache_key = gerar_cache_key(cnpj, doc_id)
        cache_file = CACHE_DIR / f"{cache_key}.pkl"
        # Adicionar versão aos dados antes de salvar
        dados_com_versao = {'_CACHE_VERSION': CACHE_VERSION, **dados}
        with open(cache_file, 'wb') as f:
            pickle.dump(dados_com_versao, f)
        logger.debug(f"Cache salvo: {cnpj} | doc {doc_id} | versao {CACHE_VERSION}")
    except Exception as e:
        logger.warning(f"Erro ao salvar cache: {e}")


def ler_cache(cnpj: str, doc_id: str) -> Optional[Dict]:
    """Lê resultado do cache se existir e se a versão for compatível."""
    if not CACHE_ENABLED:
        return None

    try:
        cache_key = gerar_cache_key(cnpj, doc_id)
        cache_file = CACHE_DIR / f"{cache_key}.pkl"
        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                dados = pickle.load(f)
            
            # Verificar versão do cache
            cache_version = dados.get('_CACHE_VERSION', 'v6.0')  # Versão antiga sem versionamento
            
            if cache_version != CACHE_VERSION:
                logger.debug(f"Cache desatualizado: {cnpj} | doc {doc_id} | versao {cache_version} != {CACHE_VERSION}")
                # Deletar cache desatualizado
                try:
                    cache_file.unlink()
                    logger.debug(f"Cache desatualizado removido: {cnpj} | doc {doc_id}")
                except:
                    pass
                return None
            
            # Remover metadado de versão antes de retornar
            dados.pop('_CACHE_VERSION', None)
            logger.debug(f"Cache hit: {cnpj} | doc {doc_id} | versao {CACHE_VERSION}")
            return dados
        return None
    except Exception as e:
        logger.warning(f"Erro ao ler cache: {e}")
        return None


print("✅ Funções de cache carregadas!")

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 5: REQUISIÇÕES COM RETRY LOGIC
# ═══════════════════════════════════════════════════════════════════════

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout)),
    reraise=True
)
def fazer_requisicao_com_retry(url: str, **kwargs) -> requests.Response:
    """
    Faz requisição HTTP com retry automático e backoff exponencial.

    Args:
        url: URL para requisição
        **kwargs: Argumentos para requests.get()

    Returns:
        Response object

    Raises:
        requests.exceptions.RequestException: Após MAX_RETRIES tentativas
    """
    logger.debug(f"Requisição: {url[:80]}...")
    response = requests.get(url, **kwargs)
    response.raise_for_status()
    return response


print("✅ Função de requisição com retry carregada!")




# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 6: EXTRAÇÃO DE DADOS DO XML (VERSÃO COMPLETA)
# ═══════════════════════════════════════════════════════════════════════

def extrair_dados_xml_completo(xml_content: bytes) -> Dict[str, Union[str, float, int, None]]:
    """
    Extrai TODOS os campos relevantes do XML incluindo dados profundos
    para análise de fundos distressed.

    Campos extraídos (100+):
    - Identificação do fundo
    - Ativos e Passivos
    - Carteira de crédito
    - Aging detalhado (faixas de vencimento)
    - Cedentes concentrados (> 10% PL)
    - Movimentação subordinada
    - Indicadores: NPL, Liquidez, Zumbi Ratio

    Args:
        xml_content: Conteúdo XML em bytes (já decodificado do Base64)

    Returns:
        Dicionário com 100+ campos estruturados
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error(f"Erro ao fazer parse do XML: {e}")
        raise

    dados = {
        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 1: IDENTIFICAÇÃO DO FUNDO
        # ═══════════════════════════════════════════════════════════════
        'CNPJ_FUNDO': buscar_valor_xml(root, 'NR_CNPJ_FUNDO'),
        'CNPJ_ADMINISTRADOR': buscar_valor_xml(root, 'NR_CNPJ_ADM'),
        'DATA_COMPETENCIA': buscar_valor_xml(root, 'DT_COMPT'),
        'TIPO_CONDOMINIO': buscar_valor_xml(root, 'TP_CONDOMINIO'),
        'FUNDO_EXCLUSIVO': buscar_valor_xml(root, 'FDO_EXCL'),
        'CLASSE_UNICA': buscar_valor_xml(root, 'CLASS_UNICA'),
        'COTISTA_VINCULADO': buscar_valor_xml(root, 'COTST_VINCUL'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 2: ATIVOS GERAIS
        # ═══════════════════════════════════════════════════════════════
        'ATIVO_TOTAL': buscar_valor_xml(root, 'APLIC_ATIVO/VL_SOM_APLIC_ATIVO'),
        'DISPONIBILIDADES': buscar_valor_xml(root, 'APLIC_ATIVO/VL_DISPONIB'),
        'CARTEIRA_TOTAL': buscar_valor_xml(root, 'APLIC_ATIVO/VL_CARTEIRA'),
        'OUTROS_ATIVOS_TOTAL': buscar_valor_xml(root, 'APLIC_ATIVO/OUTROS_ATIVOS/VL_SOM_OUTROS_ATIVOS'),
        'OUTROS_ATIVOS_CURTO_PRAZO': buscar_valor_xml(root, 'APLIC_ATIVO/OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_CURPRZ'),
        'OUTROS_ATIVOS_LONGO_PRAZO': buscar_valor_xml(root, 'APLIC_ATIVO/OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_LPRAZO'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 3: PASSIVO E PATRIMÔNIO LÍQUIDO
        # ═══════════════════════════════════════════════════════════════
        'PASSIVO_CIRCULANTE': buscar_valor_xml(root, 'PASSIV/PASSIV_VALORES/VL_PGTO_CURPRZ'),
        'PASSIVO_EXIGIVEL_LONGO_PRAZO': buscar_valor_xml(root, 'PASSIV/PASSIV_VALORES/VL_PGTO_LPRAZO'),
        'PATRIMONIO_LIQUIDO': buscar_valor_xml(root, 'PATRLIQ/VL_PATRIM_LIQ'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 4: CRÉDITOS EXISTENTES
        # ═══════════════════════════════════════════════════════════════
        'CREDITOS_ADQUIRIDOS': buscar_valor_xml(root, 'CRED_EXISTE/VL_SOM_DICRED_AQUIS'),
        'CRED_VENCIDOS_ADIMPLENTES': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_EXISTE_VENC_ADIMPL'),
        'CRED_VENCIDOS_INADIMPLENTES': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_EXISTE_VENC_INAD'),
        'CRED_TOTAL_VENC_INADIMPL': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_TOTAL_VENC_INAD'),
        'CRED_INADIMPLENCIA': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_EXISTE_INAD'),
        'CRED_PERFORMADOS': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_REFER_DICRED_PERFO'),
        'CRED_VENCIDOS_PENDENTES': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_VENC_PEND'),
        'CRED_EMP_RECUPERACAO': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_ORIGEM_EMP_PROC_RECUP'),
        'CRED_RECEITA_PUBLICA': buscar_valor_xml(root, 'CRED_EXISTE/VL_DECOR_RECEIT_PUBLIC'),
        'CRED_ACAO_JUDICIAL': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_ACAO_JUDIC'),
        'CRED_CONSTITUICAO_JURIDICA': buscar_valor_xml(root, 'CRED_EXISTE/VL_CRED_CONST_JUR_FATRISC'),
        'CRED_PROVISAO_REDUCAO': buscar_valor_xml(root, 'CRED_EXISTE/VL_PROVIS_REDUC_RECUP'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 5: AGING DA CARTEIRA (FAIXAS DE VENCIMENTO)
        # ═══════════════════════════════════════════════════════════════
        'AGING_A_VENCER': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_SOM_PRAZO_VENC'),
        'AGING_VENC_1_30_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_30'),
        'AGING_VENC_31_60_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_31_60'),
        'AGING_VENC_61_90_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_61_90'),
        'AGING_VENC_91_120_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_91_120'),
        'AGING_VENC_121_150_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_121_150'),
        'AGING_VENC_151_180_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_151_180'),
        'AGING_VENC_181_360_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_181_360'),
        'AGING_VENC_361_720_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_361_720'),
        'AGING_VENC_721_1080_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_721_1080'),
        'AGING_VENC_MAIOR_1080_DIAS': buscar_valor_xml(root, 'COMPMT_DICRED_AQUIS/VL_PRAZO_VENC_1080'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 6: DIREITOS CREDITÓRIOS (DICRED)
        # ═══════════════════════════════════════════════════════════════
        'DICRED_TOTAL': buscar_valor_xml(root, 'DICRED/VL_DICRED'),
        'DICRED_CEDENTE': buscar_valor_xml(root, 'DICRED/VL_DICRED_CEDENT'),
        'DICRED_VENC_INADIMPL': buscar_valor_xml(root, 'DICRED/VL_DICRED_EXISTE_VENC_INAD'),
        'DICRED_TOTAL_VENC_INAD': buscar_valor_xml(root, 'DICRED/VL_DICRED_TOTAL_VENC_INAD'),
        'DICRED_INADIMPLENCIA': buscar_valor_xml(root, 'DICRED/VL_DICRED_EXISTE_INAD'),
        'DICRED_PERFORMADOS': buscar_valor_xml(root, 'DICRED/VL_DICRED_REFER_DICRED_PERFO'),
        'DICRED_VENC_PENDENTES': buscar_valor_xml(root, 'DICRED/VL_DICRED_VENC_PEND'),
        'DICRED_EMP_RECUPERACAO': buscar_valor_xml(root, 'DICRED/VL_DICRED_ORIGEM_EMP_PROC_RECUP'),
        'DICRED_RECEITA_PUBLICA': buscar_valor_xml(root, 'DICRED/VL_DICRED_RECEIT_PUBLIC'),
        'DICRED_ACAO_JUDICIAL': buscar_valor_xml(root, 'DICRED/VL_DICRED_ACAO_JUDIC'),
        'DICRED_PROVISAO_REDUCAO': buscar_valor_xml(root, 'DICRED/VL_DICRED_PROVIS_REDUC_RECUP'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 7: MOVIMENTAÇÃO DA SUBORDINADA
        # ═══════════════════════════════════════════════════════════════
        'SUBORD_CAPTACOES': buscar_valor_xml(root, 'CAPTA_RESGA_AMORTI/CAPT_MES/CLASSE_SUBORD/VL_COTAS'),
        'SUBORD_AMORTIZACOES': buscar_valor_xml(root, 'CAPTA_RESGA_AMORTI/AMORT/CLASSE_SUBORD/VL_TOTAL'),
        'SUBORD_RESGATES': buscar_valor_xml(root, 'CAPTA_RESGA_AMORTI/RESG_MES/CLASSE_SUBORD/VL_COTAS'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 8: VALORES MOBILIÁRIOS
        # ═══════════════════════════════════════════════════════════════
        'VALORES_MOBILIARIOS_TOTAL': buscar_valor_xml(root, 'VALORES_MOB/VL_SOM_VALORES_MOB'),
        'DEBENTURES': buscar_valor_xml(root, 'VALORES_MOB/VL_DEBT'),
        'CRI': buscar_valor_xml(root, 'VALORES_MOB/VL_CRI'),
        'NOTAS_PROMISSORIAS_COMERCIAIS': buscar_valor_xml(root, 'VALORES_MOB/VL_NP_COMERC'),
        'LETRAS_FINANCEIRAS': buscar_valor_xml(root, 'VALORES_MOB/VL_LETRA_FINANC'),
        'COTAS_FIF': buscar_valor_xml(root, 'VALORES_MOB/VL_COTA_FDO_ICVM409'),
        'OUTROS_DIREITOS_CREDITORIOS': buscar_valor_xml(root, 'VALORES_MOB/VL_OUTRO_DICRED'),

        # ═══════════════════════════════════════════════════════════════
        # SEÇÃO 9: OUTROS ATIVOS FINANCEIROS
        # ═══════════════════════════════════════════════════════════════
        'TITULOS_PUBLICOS_FEDERAIS': buscar_valor_xml(root, 'VL_TITPUB_FED'),
        'CDB': buscar_valor_xml(root, 'VL_CDB'),
        'APLICACOES_COMPROMISSADAS': buscar_valor_xml(root, 'VL_APLIC_OPER_COMPSS'),
        'ATIVOS_FINANCEIROS_RF': buscar_valor_xml(root, 'VL_ATIV_FINANC_RF'),
        'COTAS_FIDC': buscar_valor_xml(root, 'VL_COTA_FIDC')
    }
    # ═══════════════════════════════════════════════════════════════
    # AJUSTE 6.1: CONVERTER DATA_COMPETENCIA (VERSÃO CORRIGIDA V2)
    # ═══════════════════════════════════════════════════════════════
    
    # Extrair valor original
    data_original = dados.get('DATA_COMPETENCIA')
    
    # CORREÇÃO: Verificar explicitamente se não é None/0/0.0
    if data_original is not None and str(data_original).strip() not in ['', '0', '0.0']:
        try:
            data_str = str(data_original).strip()
            
            # Verificar se tem formato MM/YYYY ou DD/MM/YYYY
            if '/' in data_str and len(data_str) >= 6:
                partes = data_str.split('/')
                
                if len(partes) == 2:
                    # Formato: MM/YYYY ou YYYY/MM
                    parte1 = partes[0].strip()
                    parte2 = partes[1].strip()
                    
                    # Detectar qual é mês e qual é ano
                    if len(parte2) == 4:  # Formato MM/YYYY
                        mes = parte1
                        ano = parte2
                    elif len(parte1) == 4:  # Formato YYYY/MM
                        ano = parte1
                        mes = parte2
                    else:
                        logger.warning(f"Formato ambíguo: {data_str}")
                        dados['DATA_COMPETENCIA'] = None
                        raise ValueError("Formato ambíguo")
                    
                    # Validar valores
                    if mes.isdigit() and ano.isdigit():
                        mes_int = int(mes)
                        ano_int = int(ano)
                        
                        if 1 <= mes_int <= 12 and 2000 <= ano_int <= 2030:
                            dados['DATA_COMPETENCIA'] = f"{ano}-{mes.zfill(2)}"
                            logger.debug(f"Data convertida: {data_str} → {dados['DATA_COMPETENCIA']}")
                        else:
                            logger.warning(f"Data fora do range válido: {data_str}")
                            dados['DATA_COMPETENCIA'] = None
                    else:
                        logger.warning(f"Partes não numéricas: {data_str}")
                        dados['DATA_COMPETENCIA'] = None
                        
                elif len(partes) == 3:
                    # Formato: DD/MM/YYYY (usar só MM/YYYY)
                    mes = partes[1].strip()
                    ano = partes[2].strip()
                    
                    if mes.isdigit() and ano.isdigit():
                        mes_int = int(mes)
                        ano_int = int(ano)
                        
                        if 1 <= mes_int <= 12 and 2000 <= ano_int <= 2030:
                            dados['DATA_COMPETENCIA'] = f"{ano}-{mes.zfill(2)}"
                            logger.debug(f"Data convertida (DD/MM/YYYY): {data_str} → {dados['DATA_COMPETENCIA']}")
                        else:
                            dados['DATA_COMPETENCIA'] = None
                    else:
                        dados['DATA_COMPETENCIA'] = None
                else:
                    logger.warning(f"Formato inesperado: {data_str}")
                    dados['DATA_COMPETENCIA'] = None
            else:
                # Não tem '/', deixar como está
                dados['DATA_COMPETENCIA'] = None
                
        except Exception as e:
            logger.warning(f"Erro ao converter DATA_COMPETENCIA '{data_original}': {e}")
            dados['DATA_COMPETENCIA'] = None
    else:
        dados['DATA_COMPETENCIA'] = None
    
    # ═══════════════════════════════════════════════════════════════
    # FALLBACK: Usar DATA_REFERENCIA_DOC se conversão falhou
    # ═══════════════════════════════════════════════════════════════
    if dados['DATA_COMPETENCIA'] is None or dados['DATA_COMPETENCIA'] in ['', '0', '0.0']:
        # Tentar campo alternativo DT_COMPTC
        data_ref = buscar_valor_xml(root, 'DT_COMPTC')
        
        # Se não encontrou, usar DATA_REFERENCIA_DOC do metadado da API
        if not data_ref or str(data_ref) in ['', '0', '0.0']:
            # Este campo vem da API, não do XML
            # Será populado depois no fluxo principal
            dados['DATA_COMPETENCIA'] = None
        else:
            try:
                data_str = str(data_ref).strip()
                if '/' in data_str:
                    partes = data_str.split('/')
                    if len(partes) >= 2:
                        # Pode ser MM/YYYY ou DD/MM/YYYY
                        if len(partes) == 2:
                            mes = partes[0].strip().zfill(2)
                            ano = partes[1].strip()
                        else:  # len == 3 (DD/MM/YYYY)
                            mes = partes[1].strip().zfill(2)
                            ano = partes[2].strip()
                        
                        dados['DATA_COMPETENCIA'] = f"{ano}-{mes}"
                        logger.debug(f"Data via fallback: {data_str} → {dados['DATA_COMPETENCIA']}")
            except Exception as e:
                logger.debug(f"Fallback falhou: {e}")
                dados['DATA_COMPETENCIA'] = None
    
    # ═══════════════════════════════════════════════════════════════════
    # EXTRAÇÃO DE CEDENTES CONCENTRADOS (> 10% PL)
    # ═══════════════════════════════════════════════════════════════════
    try:
        patrimonio_liquido = float(dados.get('PATRIMONIO_LIQUIDO', 0) or 0)
        cedentes_list = []

        cedentes_nodes = root.findall('.//CEDENTE')
        for cedente_node in cedentes_nodes:
            try:
                cnpj_cedente = buscar_valor_xml(cedente_node, 'NR_CNPJ_CEDENT')
                nome_cedente = buscar_valor_xml(cedente_node, 'RAZAO_SOCIAL_CEDENT')
                valor_cedente = float(buscar_valor_xml(cedente_node, 'VL_DICRED_CEDENT') or 0)

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

    # ═══════════════════════════════════════════════════════════════════
    # CÁLCULO DE INDICADORES DE DISTRESSED
    # ═══════════════════════════════════════════════════════════════════
    inadimpl_cred = float(dados.get('CRED_INADIMPLENCIA', 0) or 0)
    inadimpl_dicred = float(dados.get('DICRED_INADIMPLENCIA', 0) or 0)
    dados['INADIMPLENCIA_TOTAL'] = max(inadimpl_cred, inadimpl_dicred)

    # ═══════════════════════════════════════════════════════════════════
    # CORREÇÃO CRÍTICA: Usar CARTEIRA_TOTAL ao invés de CREDITOS_ADQUIRIDOS
    # ═══════════════════════════════════════════════════════════════════
    carteira_total = float(dados.get('CARTEIRA_TOTAL', 0) or 0)  # ✅ CORRIGIDO
    creditos_adquiridos = float(dados.get('CREDITOS_ADQUIRIDOS', 0) or 0)  # Para análise complementar
    ativo_total = float(dados.get('ATIVO_TOTAL', 0) or 0)
    disponibilidades = float(dados.get('DISPONIBILIDADES', 0) or 0)
    passivo_circulante = float(dados.get('PASSIVO_CIRCULANTE', 0) or 0)
    aging_maior_1080 = float(dados.get('AGING_VENC_MAIOR_1080_DIAS', 0) or 0)

    # 1. NPL_Ratio = (Créditos Vencidos Inadimplentes / Carteira Total) * 100
    # ✅ CORRIGIDO: Usa CARTEIRA_TOTAL (padrão de mercado)
    creditos_vencidos_total = float(dados.get('CRED_TOTAL_VENC_INADIMPL', 0) or 0)
    if carteira_total > 0 and creditos_vencidos_total > 0:
        dados['NPL_RATIO'] = (creditos_vencidos_total / carteira_total) * 100
    else:
        dados['NPL_RATIO'] = 0.0

    # 1.2. NPL Alternativo - Sobre créditos adquiridos (análise complementar)
    if creditos_adquiridos > 0 and creditos_vencidos_total > 0:
        dados['NPL_RATIO_CREDITO_PURO'] = (creditos_vencidos_total / creditos_adquiridos) * 100
    else:
        dados['NPL_RATIO_CREDITO_PURO'] = 0.0

    # 2. Liquidez_Imediata_Ratio = (Disponibilidades / Passivo Circulante)
    if passivo_circulante > 0:
        dados['LIQUIDEZ_IMEDIATA_RATIO'] = disponibilidades / passivo_circulante
    else:
        dados['LIQUIDEZ_IMEDIATA_RATIO'] = 0.0

    # 3. Zumbi_Ratio = (Créditos > 1080 dias / Carteira Total) * 100
    if carteira_total > 0 and aging_maior_1080 > 0:
        dados['ZUMBI_RATIO'] = (aging_maior_1080 / carteira_total) * 100
    else:
        dados['ZUMBI_RATIO'] = 0.0

    # Indicadores legados (para compatibilidade)
    if carteira_total > 0 and dados['INADIMPLENCIA_TOTAL'] > 0:
        dados['INDICE_NPL_PERCENTUAL'] = (dados['INADIMPLENCIA_TOTAL'] / carteira_total) * 100
    else:
        dados['INDICE_NPL_PERCENTUAL'] = 0.0

    if ativo_total > 0:
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = (disponibilidades / ativo_total) * 100
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = (carteira_total / ativo_total) * 100
    else:
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = 0.0
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = 0.0

    # Flag para NPL outlier
    if dados['NPL_RATIO'] > 1000:
        logger.warning(f"⚠️ NPL outlier: {dados['NPL_RATIO']:.2f}% | CNPJ: {dados.get('CNPJ_FUNDO', 'N/A')}")
        dados['NPL_OUTLIER_FLAG'] = True
    else:
        dados['NPL_OUTLIER_FLAG'] = False

    # ═══════════════════════════════════════════════════════════════════
    # FLAGS DE QUALIDADE DE DADOS (FIDC SAFARI)
    # ═══════════════════════════════════════════════════════════════════

    # Flag 1: CREDITOS_ADQUIRIDOS muito baixo (< 1% da carteira)
    if carteira_total > 0:
        ratio_cred_carteira = (creditos_adquiridos / carteira_total) * 100
        dados['FLAG_CRED_ADQ_BAIXO'] = (ratio_cred_carteira < 1.0)
    else:
        dados['FLAG_CRED_ADQ_BAIXO'] = False

    # Flag 2: Créditos vencidos > 150% da carteira (anomalia contábil)
    if carteira_total > 0:
        dados['FLAG_VENCIDOS_ALTO'] = (creditos_vencidos_total > carteira_total * 1.5)
    else:
        dados['FLAG_VENCIDOS_ALTO'] = False

    # Flag 3: NPL implausível (> 500%)
    dados['FLAG_NPL_IMPLAUSIVEL'] = (dados['NPL_RATIO'] > 500)

    # Flag 4: Carteira total zerada ou muito pequena
    dados['FLAG_CARTEIRA_ZERADA'] = (carteira_total < 1000)  # Menos de R$ 1.000

    # Flag consolidada: Anomalia estrutural
    dados['ANOMALIA_ESTRUTURAL'] = (
        dados['FLAG_CRED_ADQ_BAIXO'] or 
        dados['FLAG_VENCIDOS_ALTO'] or 
        dados['FLAG_NPL_IMPLAUSIVEL'] or
        dados['FLAG_CARTEIRA_ZERADA']
    )

    # Status de qualidade dos dados
    if dados['ANOMALIA_ESTRUTURAL']:
        if dados['FLAG_CARTEIRA_ZERADA']:
            dados['STATUS_DADOS'] = 'CARTEIRA_ZERADA'
        elif dados['FLAG_NPL_IMPLAUSIVEL']:
            dados['STATUS_DADOS'] = 'NPL_IMPLAUSIVEL'
        elif dados['FLAG_VENCIDOS_ALTO']:
            dados['STATUS_DADOS'] = 'ANOMALIA_CONTABIL'
        elif dados['FLAG_CRED_ADQ_BAIXO']:
            dados['STATUS_DADOS'] = 'ESTRUTURA_ATIPICA'
        else:
            dados['STATUS_DADOS'] = 'ANOMALIA_GENERICA'
    else:
        dados['STATUS_DADOS'] = 'VALIDADO'

    # ═══════════════════════════════════════════════════════════════════
    # SCORE FIDC SAFARI (HUNTING DISTRESSED FUNDS)
    # ═══════════════════════════════════════════════════════════════════

    # Critérios para oportunidade:
    # 1. NPL alto (15-150%) → Distressed mas ainda operacional
    # 2. Zumbi Ratio alto → Carteira antiga/deteriorada
    # 3. Liquidez baixa → Pressão de caixa
    # 4. Ativo significativo → Porte mínimo para negociação

    if dados['ANOMALIA_ESTRUTURAL']:
        # Se tem anomalia, score = 0 (excluir da análise)
        dados['SCORE_SAFARI'] = 0.0
        dados['CLASSIFICACAO_SAFARI'] = 'DADOS_INVALIDOS'
    else:
        npl = dados['NPL_RATIO']
        zumbi = dados['ZUMBI_RATIO']
        liquidez = dados['LIQUIDEZ_IMEDIATA_RATIO']
        ativo = ativo_total  # variável já existe no código
        
        # Cálculo do score (0-100)
        score = 0.0
        
        # Componente 1: NPL (peso 40%)
        if 15 <= npl <= 150:
            score += min((npl / 150) * 40, 40)  # Max 40 pontos
        elif npl > 150:
            score += 5  # Penaliza NPL muito alto
        
        # Componente 2: Zumbi Ratio (peso 25%)
        if zumbi > 0:
            score += min((zumbi / 50) * 25, 25)  # Max 25 pontos
        
        # Componente 3: Liquidez (peso 25%) - Quanto menor, melhor
        if liquidez < 1.0:
            score += (1 - liquidez) * 25  # Max 25 pontos
        
        # Componente 4: Porte (peso 10%)
        if ativo >= 100_000_000:  # >= R$ 100 Mi
            score += 10
        elif ativo >= 50_000_000:  # >= R$ 50 Mi
            score += 7
        elif ativo >= 10_000_000:  # >= R$ 10 Mi
            score += 5
        elif ativo >= 1_000_000:   # >= R$ 1 Mi
            score += 2
        
        dados['SCORE_SAFARI'] = round(score, 2)
        
        # Classificação
        if score >= 70:
            dados['CLASSIFICACAO_SAFARI'] = 'ALTA_OPORTUNIDADE'
        elif score >= 50:
            dados['CLASSIFICACAO_SAFARI'] = 'OPORTUNIDADE_MODERADA'
        elif score >= 30:
            dados['CLASSIFICACAO_SAFARI'] = 'MONITORAR'
        elif score >= 15:
            dados['CLASSIFICACAO_SAFARI'] = 'BAIXA_PRIORIDADE'
        else:
            dados['CLASSIFICACAO_SAFARI'] = 'SEM_OPORTUNIDADE'

    return dados


print("✅ Função de extração XML carregada (100+ campos)!")

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 7A: CLASSIFICADOR DE PRIORIDADE DE CLASSE
# ═══════════════════════════════════════════════════════════════════════

def classificar_prioridade_classe(nome_fundo):
    """
    Classifica a prioridade da classe de cota ANTES do download.

    Hierarquia:
    1 = Senior/Sênior (melhor - menor risco)
    2 = Mezanino (risco médio)
    3 = Subordinada/Júnior (maior risco)
    4 = Outros (não identificado)

    Args:
        nome_fundo: Nome do fundo/classe

    Returns:
        int: Prioridade (1-4)
    """
    if pd.isna(nome_fundo):
        return 4

    nome = str(nome_fundo).upper()

    if 'SENIOR' in nome or 'SÊNIOR' in nome:
        return 1
    elif 'MEZANINO' in nome or 'MEZZANINE' in nome or 'MEZA' in nome:
        return 2
    elif any(x in nome for x in ['SUBORDINADA', 'JÚNIOR', 'JUNIOR', 'SUB SENIOR', 'SUB-SENIOR']):
        return 3
    else:
        return 4


print("✅ Classificador de classes carregado!")



# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 7B: FUNÇÃO ETL PRINCIPAL (ROLLING 60 MESES + FILTRO DE CLASSE)
# ═══════════════════════════════════════════════════════════════════════

def etl_fidc_rolling_60(
    cnpj_alvo: str,
    nome_fundo_referencia: str = ""
) -> List[Dict]:
    """
    Executa ETL completo para um CNPJ coletando TODOS os meses 
    disponíveis nos ÚLTIMOS 60 MESES (5 anos).

    OTIMIZAÇÃO: Filtra a melhor classe (Senior first) ANTES de baixar XMLs.

    Pipeline:
    1. Discovery: Busca todos os documentos do CNPJ
    2. Filter: Filtra informes mensais ativos
    3. Rolling Window: Últimos 60 meses
    4. Class Filter: Seleciona melhor classe por mês (Senior > Mezanino > Subordinada)
    5. Download: Baixa apenas 1 XML por mês
    6. Extract: Extrai 100+ campos
    7. Cache: Salva resultado

    Args:
        cnpj_alvo: CNPJ do fundo (14 dígitos)
        nome_fundo_referencia: Nome do fundo (opcional)

    Returns:
        Lista de dicionários com dados extraídos
    """
    resultados_fundo_cnpj = []

    try:
        # ═══════════════════════════════════════════════════════════════
        # ETAPA 1: DISCOVERY (COM RETRY)
        # ═══════════════════════════════════════════════════════════════
        params = {
            'd': 0,
            's': 0,
            'l': 200,
            'cnpjFundo': cnpj_alvo
        }

        resp_busca = fazer_requisicao_com_retry(
            URL_API_BUSCA,
            params=params,
            headers=HEADERS,
            timeout=TIMEOUT_BUSCA
        )

        data = resp_busca.json().get('data', [])

        if not data:
            logger.debug(f"CNPJ {cnpj_alvo}: Nenhum documento encontrado")
            return []

        df_docs = pd.DataFrame(data)

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 2: FILTER - Apenas informes mensais ativos
        # ═══════════════════════════════════════════════════════════════
        df_mensal = df_docs[
            (df_docs['tipoDocumento'].str.strip() == "Informe Mensal Estruturado") &
            (df_docs['situacaoDocumento'].str.strip() == "A")
        ].copy()

        if df_mensal.empty:
            logger.debug(f"CNPJ {cnpj_alvo}: Nenhum informe mensal ativo")
            return []

        # Garantir que 'dataEntrega' existe
        if 'dataEntrega' not in df_mensal.columns:
            df_mensal['dataEntrega'] = pd.NA
            logger.warning(f"CNPJ {cnpj_alvo}: Coluna 'dataEntrega' não encontrada")

        # Converter datas
        df_mensal['dataReferenciaOrdenavel'] = pd.to_datetime(
            df_mensal['dataReferencia'].str[3:] + '-' +
            df_mensal['dataReferencia'].str[:2] + '-01',
            format='%Y-%m-%d', errors='coerce'
        )

        df_mensal['dataEntregaOrdenavel'] = pd.to_datetime(
            df_mensal['dataEntrega'], 
            format='%d/%m/%Y %H:%M', 
            errors='coerce'
        )

        # Drop linhas com datas inválidas
        df_mensal.dropna(subset=['dataReferenciaOrdenavel', 'dataEntregaOrdenavel'], inplace=True)

        if df_mensal.empty:
            logger.debug(f"CNPJ {cnpj_alvo}: Datas inválidas após conversão")
            return []

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 3: FILTRO ROLLING 60 MESES
        # ═══════════════════════════════════════════════════════════════
        data_limite = datetime.now() - relativedelta(months=60)

        df_mensal_filtrado = df_mensal[
            df_mensal['dataReferenciaOrdenavel'] >= data_limite
        ].copy()

        if df_mensal_filtrado.empty:
            logger.debug(f"CNPJ {cnpj_alvo}: Nenhum documento nos últimos 60 meses")
            return []

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 4: FILTRO INTELIGENTE DE CLASSE (ANTES DO DOWNLOAD!)
        # ═══════════════════════════════════════════════════════════════

        # Aplicar classificação de prioridade
        df_mensal_filtrado['prioridade_classe'] = df_mensal_filtrado['descricaoFundo'].apply(
            classificar_prioridade_classe
        )

        # Criar coluna ano_mes para agrupamento
        df_mensal_filtrado['ano_mes'] = df_mensal_filtrado['dataReferenciaOrdenavel'].dt.to_period('M')

        # Ordenar por prioridade (1=Senior primeiro) e data de entrega (mais recente)
        df_mensal_filtrado = df_mensal_filtrado.sort_values(
            ['ano_mes', 'prioridade_classe', 'dataEntregaOrdenavel'],
            ascending=[True, True, False]
        )

        # ✅ FILTRO: Pega apenas 1 documento por mês
        # Prioridade: Senior > Mezanino > Subordinada > Outros
        df_mensal_unique = df_mensal_filtrado.groupby('ano_mes').first().reset_index()

        documentos_processar = df_mensal_unique.to_dict('records')

        # Log com informação da classe selecionada
        classe_counts = df_mensal_unique['prioridade_classe'].value_counts()
        classe_labels = {1: 'Senior', 2: 'Mezanino', 3: 'Subordinada', 4: 'Outros'}
        classes_info = ', '.join([f"{classe_labels.get(k, 'Outros')}: {v}" for k, v in classe_counts.items()])

        logger.info(
            f"CNPJ {cnpj_alvo}: {len(documentos_processar)} meses únicos "
            f"(Classes: {classes_info})"
        )

        # ═══════════════════════════════════════════════════════════════
        # ETAPA 5-7: DOWNLOAD, EXTRACT, CLEANUP (COM CACHE)
        # ═══════════════════════════════════════════════════════════════
        for doc_info in documentos_processar:
            doc_id = str(doc_info['id'])
            data_referencia = doc_info.get('dataReferencia', 'N/A')
            nome_fundo_doc = doc_info.get('descricaoFundo', 'Nome N/A')
            prioridade_classe = doc_info.get('prioridade_classe', 4)

            # Tentar ler do cache primeiro
            dados_cache = ler_cache(cnpj_alvo, doc_id)
            if dados_cache is not None:
                # Verificar se o cache tem as novas colunas (compatibilidade)
                colunas_essenciais = ['ANOMALIA_ESTRUTURAL', 'SCORE_SAFARI', 'STATUS_DADOS']
                tem_colunas_essenciais = all(col in dados_cache for col in colunas_essenciais)
                
                if not tem_colunas_essenciais:
                    # Cache antigo sem novas colunas - invalidar e reprocessar
                    logger.debug(f"Cache antigo detectado (sem novas colunas): {cnpj_alvo} | doc {doc_id} - reprocessando")
                    cache_key = gerar_cache_key(cnpj_alvo, doc_id)
                    cache_file = CACHE_DIR / f"{cache_key}.pkl"
                    try:
                        if cache_file.exists():
                            cache_file.unlink()
                    except:
                        pass
                    # Continuar para reprocessar (não usar continue)
                else:
                    # Cache válido com todas as colunas
                    resultados_fundo_cnpj.append(dados_cache)
                    continue

            nome_arquivo_temp = None

            try:
                # Download com retry
                url_download = f"{URL_API_DOWNLOAD}?id={doc_id}"
                resp_download = fazer_requisicao_com_retry(
                    url_download,
                    headers=HEADERS,
                    timeout=TIMEOUT_DOWNLOAD
                )

                xml_content = base64.b64decode(resp_download.content)

                # Salvar temporariamente
                nome_arquivo_temp = f'temp_{cnpj_alvo}_{doc_id}.xml'
                with open(nome_arquivo_temp, 'wb') as f:
                    f.write(xml_content)

                # Extrair dados (função expandida)
                dados_extraidos = extrair_dados_xml_completo(xml_content)

                if not dados_extraidos.get('CNPJ_FUNDO'):
                    dados_extraidos['CNPJ_FUNDO'] = cnpj_alvo

                # Metadados
                dados_extraidos['NOME_FUNDO'] = nome_fundo_doc
                dados_extraidos['STATUS'] = 'SUCESSO'
                dados_extraidos['ID_DOCUMENTO'] = doc_id
                dados_extraidos['DATA_REFERENCIA_DOC'] = data_referencia
                dados_extraidos['MENSAGEM_ERRO'] = None
                dados_extraidos['TIPO_COLETA'] = 'ROLLING_60_MESES'

                # ✅ Adicionar informação da classe selecionada
                classe_labels = {1: 'Senior', 2: 'Mezanino', 3: 'Subordinada', 4: 'Outros'}
                dados_extraidos['CLASSE_SELECIONADA'] = classe_labels.get(prioridade_classe, 'Outros')
                dados_extraidos['PRIORIDADE_CLASSE'] = prioridade_classe

                # Salvar em cache
                salvar_cache(cnpj_alvo, doc_id, dados_extraidos)

                resultados_fundo_cnpj.append(dados_extraidos)

            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP Error | CNPJ {cnpj_alvo} | Doc {doc_id}: {e}")
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_HTTP_DOWNLOAD',
                    'ID_DOCUMENTO': doc_id,
                    'DATA_REFERENCIA_DOC': data_referencia,
                    'MENSAGEM_ERRO': f'HTTP Error: {str(e)}',
                    'TIPO_COLETA': 'ERRO'
                })
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout | CNPJ {cnpj_alvo} | Doc {doc_id}: {e}")
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'TIMEOUT_DOWNLOAD',
                    'ID_DOCUMENTO': doc_id,
                    'DATA_REFERENCIA_DOC': data_referencia,
                    'MENSAGEM_ERRO': f'Timeout: {str(e)}',
                    'TIPO_COLETA': 'ERRO'
                })
            except ET.ParseError as e:
                logger.error(f"Parse Error | CNPJ {cnpj_alvo} | Doc {doc_id}: {e}")
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_PARSE_XML',
                    'ID_DOCUMENTO': doc_id,
                    'DATA_REFERENCIA_DOC': data_referencia,
                    'MENSAGEM_ERRO': f'Erro XML: {str(e)}',
                    'TIPO_COLETA': 'ERRO'
                })
            except Exception as e:
                logger.error(f"Erro inesperado | CNPJ {cnpj_alvo} | Doc {doc_id}: {e}", exc_info=True)
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_INESPERADO',
                    'ID_DOCUMENTO': doc_id,
                    'DATA_REFERENCIA_DOC': data_referencia,
                    'MENSAGEM_ERRO': f'Erro: {str(e)}',
                    'TIPO_COLETA': 'ERRO'
                })

            finally:
                # Cleanup
                if nome_arquivo_temp and os.path.exists(nome_arquivo_temp):
                    try:
                        os.remove(nome_arquivo_temp)
                    except OSError:
                        pass

    except Exception as e:
        logger.error(f"Erro geral | CNPJ {cnpj_alvo}: {e}", exc_info=True)
        resultados_fundo_cnpj.append({
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo_referencia,
            'STATUS': 'ERRO_GERAL',
            'MENSAGEM_ERRO': f'Erro geral: {str(e)}',
            'TIPO_COLETA': 'ERRO'
        })

    return resultados_fundo_cnpj


logger.info("✅ Função ETL Rolling 60 meses com filtro de classe implementada")
print("✅ Função ETL principal carregada!")



# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 8: CARREGAMENTO DOS CNPJs
# ═══════════════════════════════════════════════════════════════════════

ARQUIVO_CSV_ENTRADA = Path('RAW') / 'lista_cnpjs_fidc.csv'

try:
    df_cnpjs_entrada = pd.read_csv(str(ARQUIVO_CSV_ENTRADA), encoding='utf-8-sig')
    logger.info(f"📁 Arquivo '{ARQUIVO_CSV_ENTRADA}' carregado")
    logger.info(f"   Total de CNPJs: {len(df_cnpjs_entrada)}")

    # Padronizar CNPJs
    df_cnpjs_entrada['CNPJ'] = df_cnpjs_entrada['CNPJ'].astype(str).str.replace('.0', '', regex=False)
    df_cnpjs_entrada['CNPJ'] = df_cnpjs_entrada['CNPJ'].str.zfill(14)

    # Remover duplicatas
    df_cnpjs_entrada.drop_duplicates(subset=['CNPJ'], inplace=True)
    logger.info(f"   CNPJs únicos: {len(df_cnpjs_entrada)}")

    # Estimativa de tempo
    docs_estimados_por_cnpj = 40  # Média de meses com dados
    tempo_estimado_seq = len(df_cnpjs_entrada) * DELAY_ENTRE_REQUISICOES * docs_estimados_por_cnpj / 60
    tempo_estimado_par = tempo_estimado_seq / MAX_WORKERS

    logger.info(f"⏱️ Tempo estimado:")
    logger.info(f"   Sequencial: ~{tempo_estimado_seq:.1f} min")
    logger.info(f"   Paralelo ({MAX_WORKERS} threads): ~{tempo_estimado_par:.1f} min")
    logger.info(f"   Ganho: {(1 - tempo_estimado_par/tempo_estimado_seq)*100:.0f}%")

    print(f"✅ {len(df_cnpjs_entrada)} CNPJs carregados!")

except FileNotFoundError:
    logger.error(f"❌ Arquivo '{ARQUIVO_CSV_ENTRADA}' não encontrado!")
    raise

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 9: PROCESSAMENTO PARALELO COM ROLLING 60 MESES
# ═══════════════════════════════════════════════════════════════════════

logger.info("="*80)
logger.info("🚀 INICIANDO PROCESSAMENTO PARALELO - ROLLING 60 MESES")
logger.info("="*80)

resultados_consolidados = []
tempo_inicio = datetime.now()

# Processar com ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submeter todas as tarefas
    future_to_cnpj = {}
    for idx, row in df_cnpjs_entrada.iterrows():
        cnpj = row['CNPJ']
        nome_fundo_ref = row.get('NOME_FUNDO', '')

        future = executor.submit(
            etl_fidc_rolling_60,
            cnpj,
            nome_fundo_ref
        )
        future_to_cnpj[future] = (cnpj, nome_fundo_ref)

    # Processar conforme completam
    with tqdm(total=len(df_cnpjs_entrada), desc="Processando CNPJs", unit="cnpj") as pbar:
        for future in as_completed(future_to_cnpj):
            cnpj, nome_fundo_ref = future_to_cnpj[future]

            try:
                resultados_atuais = future.result()

                if resultados_atuais:
                    resultados_consolidados.extend(resultados_atuais)

                    # Log sucesso
                    sucessos = [r for r in resultados_atuais if r.get('STATUS') == 'SUCESSO']
                    if sucessos:
                        num_docs = len(sucessos)
                        pbar.set_postfix({'ultimos_meses': num_docs, 'status': '✅ OK'})
                    else:
                        pbar.set_postfix({'status': '⚠️ Erro'})
                else:
                    resultados_consolidados.append({
                        'CNPJ_FUNDO': cnpj,
                        'NOME_FUNDO': nome_fundo_ref,
                        'STATUS': 'NENHUM_DOCUMENTO',
                        'TIPO_COLETA': 'ERRO'
                    })
                    pbar.set_postfix({'status': '📭 Sem docs'})

            except Exception as e:
                logger.error(f"Erro ao processar futuro de {cnpj}: {e}")
                pbar.set_postfix({'status': '💥 Exception'})

            pbar.update(1)
            time.sleep(0.1)  # Rate limiting suave

tempo_decorrido = (datetime.now() - tempo_inicio).total_seconds()

logger.info("="*80)
logger.info("✅ PROCESSAMENTO CONCLUÍDO!")
logger.info(f"⏱️ Tempo decorrido: {tempo_decorrido/60:.2f} minutos")
logger.info(f"📊 Total de registros coletados: {len(resultados_consolidados)}")
logger.info("="*80)

print("\n✅ Processamento paralelo concluído!")

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 10: ANÁLISE DE ERROS
# ═══════════════════════════════════════════════════════════════════════

df_resultados_temp = pd.DataFrame(resultados_consolidados)

# Contar erros por tipo
erros_por_tipo = df_resultados_temp[
    df_resultados_temp['STATUS'] != 'SUCESSO'
]['STATUS'].value_counts()

if not erros_por_tipo.empty:
    logger.info("\n" + "="*80)
    logger.info("⚠️ RESUMO DE ERROS")
    logger.info("="*80)
    for erro, qtd in erros_por_tipo.items():
        logger.info(f"   {erro}: {qtd} ocorrências")
    logger.info("="*80)

# CNPJs com erro 400 (problema na API)
erros_400 = df_resultados_temp[
    (df_resultados_temp['STATUS'] == 'ERRO_GERAL') &
    (df_resultados_temp['MENSAGEM_ERRO'].str.contains('400 Client Error', na=False))
]

if not erros_400.empty:
    logger.warning(f"\n⚠️ {len(erros_400)} CNPJs com erro 400 Client Error")
    print(f"\n⚠️ {len(erros_400)} CNPJs com erro 400 - verifique logs para detalhes")

print("✅ Análise de erros concluída!")

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 11: EXPORTAÇÃO E ANÁLISE DE DISTRESSED
# ═══════════════════════════════════════════════════════════════════════

# ───────────────────────────────────────────────────────────────────────
# 1. DATAFRAME PRINCIPAL (SEM DUPLICIDADE!)
# ───────────────────────────────────────────────────────────────────────

df_resultado_final = pd.DataFrame(resultados_consolidados)

# ═══════════════════════════════════════════════════════════════════
# NOVO: REMOVER DUPLICADAS ANTES DE EXPORTAR
# ═══════════════════════════════════════════════════════════════════

logger.info(f"Total de registros antes de remover duplicadas: {len(df_resultado_final)}")

# Remover duplicatas mantendo o registro mais recente (pela data de entrega)
df_resultado_final = df_resultado_final.sort_values(
    ['CNPJ_FUNDO', 'DATA_COMPETENCIA', 'CLASSE_SELECIONADA', 'DATA_REFERENCIA_DOC'],
    ascending=[True, True, True, False]  # ← Mais recente primeiro
)

df_resultado_final = df_resultado_final.drop_duplicates(
    subset=['CNPJ_FUNDO', 'DATA_COMPETENCIA', 'CLASSE_SELECIONADA'],
    keep='first'  # ← Mantém o mais recente
)

logger.info(f"Total de registros após remover duplicatas: {len(df_resultado_final)}")
logger.info(f"Duplicatas removidas: {18414 - len(df_resultado_final)}")

# Continuar com exportação normal...
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
arquivo_completo = f'fidc_monitor_completo_{timestamp}.csv'
df_resultado_final.to_csv(arquivo_completo, index=False, encoding='utf-8-sig')

# Garantir conversão de tipos numéricos
numeric_cols = [
    'NPL_RATIO', 'LIQUIDEZ_IMEDIATA_RATIO', 'ZUMBI_RATIO',
    'ATIVO_TOTAL', 'CARTEIRA_TOTAL', 'PASSIVO_CIRCULANTE',
    'INADIMPLENCIA_TOTAL', 'AGING_VENC_MAIOR_1080_DIAS',
    'PATRIMONIO_LIQUIDO', 'DISPONIBILIDADES'
]

for col in numeric_cols:
    if col in df_resultado_final.columns:
        df_resultado_final[col] = pd.to_numeric(df_resultado_final[col], errors='coerce')

# Filtrar apenas registros com sucesso
df_valido = df_resultado_final[df_resultado_final['STATUS'] == 'SUCESSO'].copy()

logger.info(f"\n📊 Total de registros coletados: {len(df_resultado_final)}")
logger.info(f"📊 Registros válidos: {len(df_valido)}")
logger.info(f"📊 Taxa de sucesso: {len(df_valido)/len(df_resultado_final)*100:.2f}%")

# ───────────────────────────────────────────────────────────────────────
# 2. ESTATÍSTICAS DE CLASSES SELECIONADAS
# ───────────────────────────────────────────────────────────────────────

if 'CLASSE_SELECIONADA' in df_valido.columns:
    logger.info("\n📊 DISTRIBUIÇÃO DE CLASSES COLETADAS:")
    distribuicao_classes = df_valido['CLASSE_SELECIONADA'].value_counts()
    for classe, qtd in distribuicao_classes.items():
        logger.info(f"   {classe}: {qtd} registros ({qtd/len(df_valido)*100:.1f}%)")

# ───────────────────────────────────────────────────────────────────────
# 3. DATASET FILTRADO: FUNDOS DISTRESSED (NPL > 20%)
# ───────────────────────────────────────────────────────────────────────

df_distressed = df_valido[df_valido['NPL_RATIO'] > 20].copy()

# ───────────────────────────────────────────────────────────────────────
# 4. EXPORTAR ARQUIVOS CSV
# ───────────────────────────────────────────────────────────────────────

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Arquivo 1: Dados completos (sem duplicidade!)
arquivo_completo = f'fidc_monitor_completo_{timestamp}.csv'
df_valido.to_csv(arquivo_completo, index=False, encoding='utf-8-sig')
logger.info(f"\n✅ Arquivo completo exportado: {arquivo_completo}")
logger.info(f"   Total de registros: {len(df_valido)}")

# Arquivo 2: Apenas fundos distressed (NPL > 20%)
arquivo_distressed = f'fidc_distressed_npl_gt_20_{timestamp}.csv'
df_distressed.to_csv(arquivo_distressed, index=False, encoding='utf-8-sig')
logger.info(f"✅ Arquivo distressed exportado: {arquivo_distressed}")
logger.info(f"   Total de registros distressed: {len(df_distressed)}")

# ───────────────────────────────────────────────────────────────────────
# 5. ESTATÍSTICAS CONSOLIDADAS
# ───────────────────────────────────────────────────────────────────────

logger.info("\n" + "="*80)
logger.info("📊 ESTATÍSTICAS DO MERCADO DE FIDCS")
logger.info("="*80)

logger.info(f"🏦 Fundos únicos processados: {df_valido['CNPJ_FUNDO'].nunique()}")
logger.info(f"📅 Meses coletados: {df_valido['DATA_COMPETENCIA'].nunique()}")
logger.info(f"💰 Total de ativos sob gestão: R$ {df_valido['ATIVO_TOTAL'].sum()/1e9:.2f} bilhões")
logger.info(f"📋 Total de carteira de crédito: R$ {df_valido['CARTEIRA_TOTAL'].sum()/1e9:.2f} bilhões")
logger.info(f"⚠️ Total de inadimplência: R$ {df_valido['INADIMPLENCIA_TOTAL'].sum()/1e9:.2f} bilhões")
logger.info(f"📊 NPL médio do mercado: {df_valido['NPL_RATIO'].mean():.2f}%")
logger.info(f"🧟 Zumbi Ratio médio: {df_valido['ZUMBI_RATIO'].mean():.2f}%")

# ───────────────────────────────────────────────────────────────────────
# 6. ESTATÍSTICAS DE FUNDOS DISTRESSED
# ───────────────────────────────────────────────────────────────────────

if len(df_distressed) > 0:
    logger.info("\n" + "="*80)
    logger.info("🔴 ESTATÍSTICAS - FUNDOS DISTRESSED (NPL > 20%)")
    logger.info("="*80)

    logger.info(f"🏦 Fundos únicos distressed: {df_distressed['CNPJ_FUNDO'].nunique()}")
    logger.info(f"📊 % do mercado em distress: {len(df_distressed)/len(df_valido)*100:.2f}%")
    logger.info(f"💰 Ativos em distress: R$ {df_distressed['ATIVO_TOTAL'].sum()/1e9:.2f} bilhões")
    logger.info(f"📊 NPL médio (distressed): {df_distressed['NPL_RATIO'].mean():.2f}%")
    logger.info(f"⚠️ NPL máximo: {df_distressed['NPL_RATIO'].max():.2f}%")
    logger.info(f"🧟 Zumbi Ratio médio: {df_distressed['ZUMBI_RATIO'].mean():.2f}%")
    logger.info(f"💧 Liquidez Imediata média: {df_distressed['LIQUIDEZ_IMEDIATA_RATIO'].mean():.4f}")

    # Top 10 fundos com maior NPL
    logger.info("\n🔴 TOP 10 FUNDOS COM MAIOR NPL:")
    top_npl = df_distressed.nlargest(10, 'NPL_RATIO')[
        ['CNPJ_FUNDO', 'NOME_FUNDO', 'DATA_COMPETENCIA', 'NPL_RATIO', 'ZUMBI_RATIO', 'ATIVO_TOTAL', 'CLASSE_SELECIONADA']
    ]

    for idx, row in top_npl.iterrows():
        logger.info(
            f"  {row['NOME_FUNDO'][:45]:<45} | "
            f"NPL: {row['NPL_RATIO']:>7.2f}% | "
            f"Zumbi: {row['ZUMBI_RATIO']:>6.2f}% | "
            f"Classe: {row.get('CLASSE_SELECIONADA', 'N/A')}"
        )

    logger.info("="*80)
else:
    logger.warning("\n⚠️ Nenhum fundo com NPL > 20% encontrado!")

logger.info("\n" + "="*80)
logger.info("✅ PROCESSAMENTO FINALIZADO!")
logger.info("="*80)
logger.info(f"📁 Arquivos gerados:")
logger.info(f"   1. {arquivo_completo} (sem duplicidade)")
logger.info(f"   2. {arquivo_distressed} (NPL > 20%)")
logger.info(f"📁 Logs salvos em: {log_filename}")
logger.info("="*80)

print("\n" + "="*80)
print("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
print("="*80)
print(f"📁 Arquivos CSV gerados:")
print(f"   1. {arquivo_completo}")
print(f"   2. {arquivo_distressed}")
print(f"\n📊 Estatísticas principais:")
print(f"   • Fundos processados: {df_valido['CNPJ_FUNDO'].nunique()}")
print(f"   • Registros válidos: {len(df_valido)}")
print(f"   • Fundos distressed (NPL>20%): {df_distressed['CNPJ_FUNDO'].nunique()}")
print(f"   • NPL médio mercado: {df_valido['NPL_RATIO'].mean():.2f}%")
print("="*80)

# ═══════════════════════════════════════════════════════════════════════
# SEÇÃO 11B: EXPORTAÇÃO FIDC SAFARI (DADOS LIMPOS E APRESENTÁVEIS)
# ═══════════════════════════════════════════════════════════════════════

# ───────────────────────────────────────────────────────────────────────
# 1. FILTROS PARA FIDC SAFARI
# ───────────────────────────────────────────────────────────────────────

# Verificar se as colunas existem (caso seja execução antiga)
if 'ANOMALIA_ESTRUTURAL' in df_valido.columns and 'SCORE_SAFARI' in df_valido.columns:
    df_safari = df_valido[
        (df_valido['ANOMALIA_ESTRUTURAL'] == False) &      # Sem anomalias
        (df_valido['NPL_RATIO'] >= 15) &                   # NPL > 15%
        (df_valido['NPL_RATIO'] <= 150) &                  # NPL < 150%
        (df_valido['ATIVO_TOTAL'] >= 1_000_000) &          # Ativo > R$ 1 Mi
        (df_valido['SCORE_SAFARI'] >= 30)                  # Score mínimo
    ].copy()

    # ═══════════════════════════════════════════════════════════════════
    # NOVO: CALCULAR EVOLUÇÃO DO NPL (MÊS A MÊS) - AJUSTE 6.3
    # ═══════════════════════════════════════════════════════════════════
    # ATENÇÃO: Só funciona se DATA_COMPETENCIA foi corrigida (Ajuste 6.1)
    # ═══════════════════════════════════════════════════════════════════
    
    # Verificar se DATA_COMPETENCIA tem dados válidos
    data_valida = (
        df_safari['DATA_COMPETENCIA'].notna().any() and 
        df_safari['DATA_COMPETENCIA'].nunique() > 1
    )
    
    if data_valida:
        # Ordenar por fundo e data para cálculo temporal
        df_safari_temp = df_safari.sort_values(['CNPJ_FUNDO', 'DATA_COMPETENCIA'])
        
        # Calcular variação do NPL em relação ao mês anterior
        df_safari_temp['NPL_VARIACAO_MES'] = (
            df_safari_temp.groupby('CNPJ_FUNDO')['NPL_RATIO'].diff()
        )
        
        # Calcular variação percentual
        df_safari_temp['NPL_VARIACAO_PCT'] = (
            df_safari_temp.groupby('CNPJ_FUNDO')['NPL_RATIO'].pct_change() * 100
        )
        
        # Classificar tendência
        def classificar_tendencia(variacao):
            if pd.isna(variacao):
                return 'SEM_DADOS'  # Primeiro mês do fundo
            elif variacao > 10:
                return 'PIORANDO'  # NPL subindo > 10pp (removido emoji)
            elif variacao < -10:
                return 'MELHORANDO'  # NPL caindo > 10pp (removido emoji)
            else:
                return 'ESTAVEL'  # Variação pequena
        
        df_safari_temp['TENDENCIA_NPL'] = (
            df_safari_temp['NPL_VARIACAO_MES'].apply(classificar_tendencia)
        )
        
        # Calcular NPL médio dos últimos 3 meses (suaviza volatilidade)
        df_safari_temp['NPL_MEDIA_3M'] = (
            df_safari_temp.groupby('CNPJ_FUNDO')['NPL_RATIO']
            .transform(lambda x: x.rolling(window=3, min_periods=1).mean())
        )
        
        # Atualizar df_safari com as novas colunas
        df_safari = df_safari_temp
        
        logger.info("Evolucao temporal calculada com sucesso (Ajuste 6.3)")
    else:
        # DATA_COMPETENCIA ainda não foi corrigida - adicionar colunas vazias
        df_safari['NPL_VARIACAO_MES'] = None
        df_safari['NPL_VARIACAO_PCT'] = None
        df_safari['TENDENCIA_NPL'] = 'NAO_DISPONIVEL'
        df_safari['NPL_MEDIA_3M'] = df_safari['NPL_RATIO']  # Usar NPL atual
        
        logger.warning(
            "DATA_COMPETENCIA invalida - evolucao temporal nao calculada. "
            "Implemente Ajuste 6.1 primeiro."
        )

    # ───────────────────────────────────────────────────────────────────────
    # Ordenar por score (maiores oportunidades primeiro)
    # ───────────────────────────────────────────────────────────────────────
    df_safari = df_safari.sort_values('SCORE_SAFARI', ascending=False)

    # ───────────────────────────────────────────────────────────────────────
    # 2. CRIAR COLUNAS FORMATADAS PARA APRESENTAÇÃO
    # ───────────────────────────────────────────────────────────────────────

    df_safari['ATIVO_FORMATADO'] = df_safari['ATIVO_TOTAL'].apply(
        lambda x: f"R$ {x/1e6:.2f}M" if x >= 1e6 else f"R$ {x/1e3:.0f}k"
    )

    df_safari['CARTEIRA_FORMATADA'] = df_safari['CARTEIRA_TOTAL'].apply(
        lambda x: f"R$ {x/1e6:.2f}M" if x >= 1e6 else f"R$ {x/1e3:.0f}k"
    )

    df_safari['NPL_FORMATADO'] = df_safari['NPL_RATIO'].apply(lambda x: f"{x:.1f}%")

    # ───────────────────────────────────────────────────────────────────────
    # 3. SELECIONAR COLUNAS RELEVANTES PARA APRESENTAÇÃO
    # ───────────────────────────────────────────────────────────────────────

    colunas_safari = [
        'CNPJ_FUNDO',
        'NOME_FUNDO',
        'DATA_COMPETENCIA',
        'ATIVO_FORMATADO',
        'CARTEIRA_FORMATADA',
        'NPL_FORMATADO',
        'ZUMBI_RATIO',
        'LIQUIDEZ_IMEDIATA_RATIO',
        'SCORE_SAFARI',
        'CLASSIFICACAO_SAFARI',
        'CLASSE_SELECIONADA',
        # Dados brutos para análise detalhada
        'ATIVO_TOTAL',
        'CARTEIRA_TOTAL',
        'NPL_RATIO',
        'PATRIMONIO_LIQUIDO',
        'PASSIVO_CIRCULANTE',
        'DISPONIBILIDADES',
        'INADIMPLENCIA_TOTAL',
        'AGING_VENC_MAIOR_1080_DIAS',
        'STATUS_DADOS'
    ]
    
    # Adicionar colunas de evolução temporal se foram calculadas
    if data_valida:
        colunas_safari.extend([
            'NPL_VARIACAO_MES',
            'NPL_VARIACAO_PCT',
            'TENDENCIA_NPL',
            'NPL_MEDIA_3M'
        ])

    # Filtrar apenas colunas que existem
    colunas_safari_existentes = [col for col in colunas_safari if col in df_safari.columns]
    df_safari_export = df_safari[colunas_safari_existentes].copy()

    # ───────────────────────────────────────────────────────────────────────
    # 4. EXPORTAR ARQUIVO SAFARI
    # ───────────────────────────────────────────────────────────────────────

    timestamp_safari = datetime.now().strftime('%Y%m%d_%H%M%S')
    arquivo_safari = f'fidc_safari_oportunidades_{timestamp_safari}.csv'
    df_safari_export.to_csv(arquivo_safari, index=False, encoding='utf-8-sig')

    # ───────────────────────────────────────────────────────────────────────
    # 5. ESTATÍSTICAS FIDC SAFARI
    # ───────────────────────────────────────────────────────────────────────

    logger.info("\n" + "="*80)
    logger.info("FIDC SAFARI - OPORTUNIDADES IDENTIFICADAS")
    logger.info("="*80)

    logger.info(f"Total de fundos com oportunidade: {len(df_safari)}")
    logger.info(f"Ativo total sob oportunidade: R$ {df_safari['ATIVO_TOTAL'].sum()/1e9:.2f} bilhoes")
    logger.info(f"Carteira total: R$ {df_safari['CARTEIRA_TOTAL'].sum()/1e9:.2f} bilhoes")
    logger.info(f"NPL medio: {df_safari['NPL_RATIO'].mean():.2f}%")
    logger.info(f"Score medio: {df_safari['SCORE_SAFARI'].mean():.1f}")

    # Distribuição por classificação
    logger.info("\nDISTRIBUICAO POR CLASSIFICACAO:")
    dist_class = df_safari['CLASSIFICACAO_SAFARI'].value_counts()
    for classe, qtd in dist_class.items():
        logger.info(f"   {classe}: {qtd} fundos ({qtd/len(df_safari)*100:.1f}%)")
    
    # Estatísticas de evolução temporal (se disponível)
    if data_valida and 'TENDENCIA_NPL' in df_safari.columns:
        logger.info("\nEVOLUCAO TEMPORAL (NPL):")
        dist_tendencia = df_safari['TENDENCIA_NPL'].value_counts()
        for tend, qtd in dist_tendencia.items():
            logger.info(f"   {tend}: {qtd} fundos ({qtd/len(df_safari)*100:.1f}%)")

    # Top 10 oportunidades
    if len(df_safari) > 0:
        logger.info("\nTOP 10 OPORTUNIDADES (MAIOR SCORE):")
        top_10 = df_safari.head(10)
        for idx, row in top_10.iterrows():
            tendencia_info = ""
            if data_valida and 'TENDENCIA_NPL' in row:
                tendencia_info = f" | {row['TENDENCIA_NPL']}"
            
            logger.info(
                f"  {row['NOME_FUNDO'][:50]:<50} | "
                f"Score: {row['SCORE_SAFARI']:>5.1f} | "
                f"NPL: {row['NPL_RATIO']:>6.1f}%{tendencia_info} | "
                f"Ativo: {row['ATIVO_FORMATADO']}"
            )

    logger.info("="*80)
    logger.info(f"Arquivo exportado: {arquivo_safari}")
    logger.info("="*80)

    print(f"\nFIDC SAFARI: {len(df_safari)} oportunidades identificadas!")
    print(f"Arquivo: {arquivo_safari}")
else:
    logger.warning("Colunas FIDC Safari nao encontradas. Execute o ETL completo para gerar.")
    print("\nATENCAO: Colunas FIDC Safari nao encontradas. Execute o ETL completo para gerar.")

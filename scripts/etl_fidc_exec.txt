# ═══════════════════════════════════════════════════════════════════════════════
# ETL FIDC V3 - EXTRAÇÃO COMPLETA DE DADOS FINANCEIROS (90+ CAMPOS)
# ═══════════════════════════════════════════════════════════════════════════════
# Autor: Rafael Augusto
# Data: Dezembro 2025
# Objetivo: Extrair dados completos de FIDCs via API B3 para análise em BI
# Input: lista_cnpjs_fidc.csv (441 CNPJs únicos)
# Output: base_fidc_completa.csv (90+ campos por CNPJ)
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 1: IMPORTS E CONFIGURAÇÕES
# ═══════════════════════════════════════════════════════════════════════════════

import pandas as pd
import requests
import base64
import xml.etree.ElementTree as ET
import time
import os
from typing import Dict, Optional

# Configurações da API B3
URL_API_BUSCA = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
URL_API_DOWNLOAD = "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Configurações de controle
DELAY_ENTRE_REQUISICOES = 2  # segundos (recomendado: 2-3s para evitar bloqueio)
TIMEOUT_BUSCA = 10  # segundos
TIMEOUT_DOWNLOAD = 20  # segundos

print("✅ Bibliotecas importadas com sucesso!")
print(f"📊 Configurações:")
print(f"   • Delay entre requisições: {DELAY_ENTRE_REQUISICOES}s")
print(f"   • Timeout busca: {TIMEOUT_BUSCA}s")
print(f"   • Timeout download: {TIMEOUT_DOWNLOAD}s")

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 2: FUNÇÕES AUXILIARES
# ═══════════════════════════════════════════════════════════════════════════════

def limpar_tag(tag: str) -> str:
    """
    Remove namespace XML das tags.

    Exemplo:
        {urn:fidc}VL_DISPONIB → VL_DISPONIB
    """
    return tag.split('}')[-1] if '}' in tag else tag


def converter_valor(texto: str) -> float:
    """
    Converte string no formato brasileiro para float.

    Exemplos:
        "1.234.567,89" → 1234567.89
        "123,45"       → 123.45
        ""             → 0.0
        None           → 0.0
    """
    if not texto or not str(texto).strip():
        return 0.0

    try:
        # Remove pontos (separador de milhar) e troca vírgula por ponto
        texto_limpo = str(texto).replace('.', '').replace(',', '.')
        return float(texto_limpo)
    except (ValueError, AttributeError):
        return 0.0


def buscar_valor_xml(root: ET.Element, caminho: str) -> any:
    """
    Busca um valor no XML e tenta converter para float.
    Se não conseguir, retorna como string.

    Args:
        root: Elemento raiz do XML
        caminho: Caminho XPath simplificado (ex: "CRED_EXISTE/VL_SOM_DICRED_AQUIS")

    Returns:
        float ou string ou 0.0/''
    """
    elemento = root.find(f'.//{caminho}')

    if elemento is not None and elemento.text:
        # Tentar converter para float (valores monetários)
        try:
            return converter_valor(elemento.text)
        except:
            # Se falhar, retornar como string (datas, textos, etc)
            return elemento.text.strip()

    # Retornar 0.0 para campos numéricos, '' para texto
    return 0.0 if '/' in caminho else ''


print("✅ Funções auxiliares definidas:")
print("   • limpar_tag()")
print("   • converter_valor()")
print("   • buscar_valor_xml()")

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 3: FUNÇÃO DE EXTRAÇÃO COMPLETA DO XML (90+ CAMPOS)
# ═══════════════════════════════════════════════════════════════════════════════

def extrair_dados_xml_completo(xml_content: bytes) -> Dict:
    """
    Extrai TODOS os campos relevantes do XML de Informe Mensal FIDC.

    Args:
        xml_content: Conteúdo XML em bytes (já decodificado do Base64)

    Returns:
        Dicionário com 90+ campos estruturados para análise
    """
    root = ET.fromstring(xml_content)

    # ─────────────────────────────────────────────────────────────────────────
    # EXTRAÇÃO DE DADOS POR SEÇÃO
    # ─────────────────────────────────────────────────────────────────────────

    dados = {
        # ═══ SEÇÃO 1: IDENTIFICAÇÃO DO FUNDO ═══
        'CNPJ_FUNDO': buscar_valor_xml(root, 'NR_CNPJ_FUNDO'),
        'CNPJ_ADMINISTRADOR': buscar_valor_xml(root, 'NR_CNPJ_ADM'),
        'DATA_COMPETENCIA': buscar_valor_xml(root, 'DT_COMPT'),
        'TIPO_CONDOMINIO': buscar_valor_xml(root, 'TP_CONDOMINIO'),
        'FUNDO_EXCLUSIVO': buscar_valor_xml(root, 'FDO_EXCL'),
        'CLASSE_UNICA': buscar_valor_xml(root, 'CLASS_UNICA'),
        'COTISTA_VINCULADO': buscar_valor_xml(root, 'COTST_VINCUL'),

        # ═══ SEÇÃO 2: ATIVOS GERAIS ═══
        'ATIVO_TOTAL': buscar_valor_xml(root, 'VL_SOM_APLIC_ATIVO'),
        'DISPONIBILIDADES': buscar_valor_xml(root, 'VL_DISPONIB'),
        'CARTEIRA_TOTAL': buscar_valor_xml(root, 'VL_CARTEIRA'),
        'OUTROS_ATIVOS_TOTAL': buscar_valor_xml(root, 'VL_SOM_OUTROS_ATIVOS'),
        'OUTROS_ATIVOS_CURTO_PRAZO': buscar_valor_xml(root, 'OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_CURPRZ'),
        'OUTROS_ATIVOS_LONGO_PRAZO': buscar_valor_xml(root, 'OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_LPRAZO'),

        # ═══ SEÇÃO 3: CRÉDITOS EXISTENTES (Principal fonte de inadimplência) ═══
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

        # ═══ SEÇÃO 4: DIREITOS CREDITÓRIOS (DICRED) ═══
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

        # ═══ SEÇÃO 5: VALORES MOBILIÁRIOS ═══
        'VALORES_MOBILIARIOS_TOTAL': buscar_valor_xml(root, 'VALORES_MOB/VL_SOM_VALORES_MOB'),
        'DEBENTURES': buscar_valor_xml(root, 'VALORES_MOB/VL_DEBT'),
        'CRI': buscar_valor_xml(root, 'VALORES_MOB/VL_CRI'),
        'NOTAS_PROMISSORIAS_COMERCIAIS': buscar_valor_xml(root, 'VALORES_MOB/VL_NP_COMERC'),
        'LETRAS_FINANCEIRAS': buscar_valor_xml(root, 'VALORES_MOB/VL_LETRA_FINANC'),
        'COTAS_FIF': buscar_valor_xml(root, 'VALORES_MOB/VL_CLS_COTA_FIF'),
        'OUTROS_DIREITOS_CREDITORIOS': buscar_valor_xml(root, 'VALORES_MOB/VL_OUTRO_DICRED'),

        # ═══ SEÇÃO 6: OUTROS ATIVOS FINANCEIROS ═══
        'TITULOS_PUBLICOS_FEDERAIS': buscar_valor_xml(root, 'VL_TITPUB_FED'),
        'CDB': buscar_valor_xml(root, 'VL_CDB'),
        'APLICACOES_COMPROMISSADAS': buscar_valor_xml(root, 'VL_APLIC_OPER_COMPSS'),
        'ATIVOS_FINANCEIROS_RF': buscar_valor_xml(root, 'VL_ATIV_FINANC_RF'),
        'COTAS_FIDC': buscar_valor_xml(root, 'VL_COTA_FIDC'),

        # ═══ SEÇÃO 7: MERCADO DE DERIVATIVOS ═══
        'DERIVATIVOS_TOTAL': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_SOM_MERC_DERIVATIVO'),
        'TERMO_COMPRADOR': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_TERMO_POS_COMPRD'),
        'OPCOES_TITULAR': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_OP_POS_TITUL'),
        'FUTUROS_AJUSTE_POSITIVO': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_FUT_AJUST_POSIT'),
        'SWAP_A_RECEBER': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_DIFER_SWAP_RECEB'),
        'COBERTURA_PRESTADA': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_COBERT_PREST'),
        'DEPOSITOS_MARGEM': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_DEPOS_MARGEM'),

        # ═══ SEÇÃO 8: SEGMENTAÇÃO DA CARTEIRA ═══
        'CARTEIRA_SEGMENTADA_TOTAL': buscar_valor_xml(root, 'CART_SEGMT/VL_SOM_CART_SEGMT'),
        'SEGMT_INDUSTRIAL': buscar_valor_xml(root, 'CART_SEGMT/VL_IND'),
        'SEGMT_MERCADO_IMOBILIARIO': buscar_valor_xml(root, 'CART_SEGMT/VL_MERC_IMOBIL'),
        'SEGMT_AGRONEGOCIO': buscar_valor_xml(root, 'CART_SEGMT/VL_AGRONEG'),
        'SEGMT_CARTAO_CREDITO': buscar_valor_xml(root, 'CART_SEGMT/VL_CART_CRED'),
        'SEGMT_ACAO_JUDICIAL': buscar_valor_xml(root, 'CART_SEGMT/VL_ACAO_JUDIC'),
        'SEGMT_PROPRIEDADE_INTELECTUAL': buscar_valor_xml(root, 'CART_SEGMT/VL_PROPRD_MARCA_PATENT'),

        # ─── Subsegmento: COMERCIAL ───
        'SEGMT_COMERCIAL_TOTAL': buscar_valor_xml(root, 'SEGMT_COMERC/VL_SOM_SEGMT_COMERC'),
        'SEGMT_COMERCIO': buscar_valor_xml(root, 'SEGMT_COMERC/VL_COMERC'),
        'SEGMT_COMERCIO_VAREJO': buscar_valor_xml(root, 'SEGMT_COMERC/VL_COMERC_VARJ'),
        'SEGMT_ARREND_MERCANTIL': buscar_valor_xml(root, 'SEGMT_COMERC/VL_ARREND_MERCNT'),

        # ─── Subsegmento: SERVIÇOS ───
        'SEGMT_SERVICOS_TOTAL': buscar_valor_xml(root, 'SEGMT_SERV/VL_SOM_SEGMT_SERV'),
        'SEGMT_SERVICOS_GERAIS': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV'),
        'SEGMT_SERVICOS_PUBLICOS': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_PUBLIC'),
        'SEGMT_SERVICOS_EDUCACAO': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_EDUC'),
        'SEGMT_SERVICOS_ENTRETENIMENTO': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_ENTRETEN'),

        # ─── Subsegmento: FINANCEIRO ───
        'SEGMT_FINANCEIRO_TOTAL': buscar_valor_xml(root, 'SEGMT_FINANC/VL_SOM_SEGMT_FINANC'),
        'SEGMT_FINANC_CREDITO_PESSOA': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_PESSOA'),
        'SEGMT_FINANC_CONSIGNADO': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_PESSOA_CONSIG'),
        'SEGMT_FINANC_CORPORATIVO': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_CORPOR'),
        'SEGMT_FINANC_MIDDLE_MARKET': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_MMARKET'),
        'SEGMT_FINANC_VEICULOS': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_VEICL'),
        'SEGMT_FINANC_IMOB_EMPRESARIAL': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_IMOBIL_EMPSRL'),
        'SEGMT_FINANC_IMOB_RESIDENCIAL': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_IMOBIL_RESID'),
        'SEGMT_FINANC_OUTROS': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_OUTRO'),

        # ─── Subsegmento: FACTORING ───
        'SEGMT_FACTORING_TOTAL': buscar_valor_xml(root, 'SEGMT_FACT/VL_SOM_SEGMT_FACT'),
        'SEGMT_FACTORING_PESSOA': buscar_valor_xml(root, 'SEGMT_FACT/VL_FACT_PESSOA'),
        'SEGMT_FACTORING_CORPORATIVO': buscar_valor_xml(root, 'SEGMT_FACT/VL_FACT_CORPOR'),

        # ─── Subsegmento: SETOR PÚBLICO ───
        'SEGMT_SETOR_PUBLICO_TOTAL': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SOM_SEGMT_SETOR_PUBLIC'),
        'SEGMT_PRECATORIOS': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_PRECAT'),
        'SEGMT_CREDITOS_TRIBUTARIOS': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_CRED_TRIBUT'),
        'SEGMT_ROYALTIES': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_ROYA'),
        'SEGMT_SETOR_PUBLICO_OUTROS': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_OUTRO'),
    }

    # ─────────────────────────────────────────────────────────────────────────
    # CÁLCULO DE INDICADORES CRÍTICOS
    # ─────────────────────────────────────────────────────────────────────────

    # Inadimplência consolidada (prioriza maior valor entre CRED e DICRED)
    inadimpl_cred = dados['CRED_INADIMPLENCIA']
    inadimpl_dicred = dados['DICRED_INADIMPLENCIA']
    dados['INADIMPLENCIA_TOTAL'] = max(inadimpl_cred, inadimpl_dicred)

    # Carteira de crédito para cálculo de NPL
    carteira_credito = dados['CREDITOS_ADQUIRIDOS']

    # Índice de NPL (Non-Performing Loans) - CORRIGIDO
    if carteira_credito > 0 and dados['INADIMPLENCIA_TOTAL'] > 0:
        dados['INDICE_NPL_PERCENTUAL'] = (dados['INADIMPLENCIA_TOTAL'] / carteira_credito) * 100
    else:
        dados['INDICE_NPL_PERCENTUAL'] = 0.0

    # Taxa de liquidez imediata (Disponibilidades / Ativo Total)
    if dados['ATIVO_TOTAL'] > 0:
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = (dados['DISPONIBILIDADES'] / dados['ATIVO_TOTAL']) * 100
    else:
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = 0.0

    # Concentração em crédito (Carteira / Ativo Total)
    if dados['ATIVO_TOTAL'] > 0:
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = (carteira_credito / dados['ATIVO_TOTAL']) * 100
    else:
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = 0.0

    return dados


print("✅ Função de extração completa definida:")
print("   • 90+ campos extraídos")
print("   • 4 indicadores calculados automaticamente")
print("   • Tratamento robusto de valores ausentes")

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 4: FUNÇÃO ETL COMPLETA
# ═══════════════════════════════════════════════════════════════════════════════

def etl_fidc_completo(cnpj_alvo: str, nome_fundo: str = "") -> Dict:
    """
    Executa o pipeline ETL completo para um CNPJ.

    Workflow:
        1. Discovery: Busca documentos do fundo na API B3
        2. Download: Baixa o XML do informe mais recente
        3. Parse: Decodifica e processa o XML
        4. Extract: Extrai 90+ campos estruturados
        5. Cleanup: Remove arquivo temporário

    Args:
        cnpj_alvo: CNPJ do fundo (string com 14 dígitos)
        nome_fundo: Nome do fundo (opcional, para logs mais legíveis)

    Returns:
        Dicionário com todos os dados extraídos + status
    """

    nome_arquivo_temp = None

    try:
        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 1: DISCOVERY - Buscar documentos disponíveis
        # ─────────────────────────────────────────────────────────────────────

        params = {
            'd': 0,         # Documento
            's': 0,         # Start (paginação)
            'l': 100,       # Limit (últimos 100)
            'cnpjFundo': cnpj_alvo
        }

        resp_busca = requests.get(
            URL_API_BUSCA,
            params=params,
            headers=HEADERS,
            timeout=TIMEOUT_BUSCA
        )
        resp_busca.raise_for_status()

        data = resp_busca.json().get('data', [])

        if not data:
            return {
                'CNPJ_FUNDO': cnpj_alvo,
                'NOME_FUNDO': nome_fundo,
                'STATUS': 'SEM_DOCUMENTOS',
                'MENSAGEM_ERRO': 'Nenhum documento encontrado para este CNPJ'
            }

        # Filtrar apenas Informes Mensais
        df_docs = pd.DataFrame(data)
        mask_mensal = (
            df_docs['tipoDocumento'].str.contains("Mensal", case=False, na=False) |
            df_docs['categoriaDocumento'].str.contains("Mensal", case=False, na=False)
        )
        df_mensal = df_docs[mask_mensal]

        if df_mensal.empty:
            return {
                'CNPJ_FUNDO': cnpj_alvo,
                'NOME_FUNDO': nome_fundo,
                'STATUS': 'SEM_INFORME_MENSAL',
                'MENSAGEM_ERRO': 'Nenhum Informe Mensal encontrado'
            }

        # Pegar o documento mais recente
        doc_id = df_mensal.iloc[0]['id']
        data_referencia = df_mensal.iloc[0].get('dataReferencia', 'N/A')

        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 2: DOWNLOAD - Baixar XML do documento
        # ─────────────────────────────────────────────────────────────────────

        url_download = f"{URL_API_DOWNLOAD}?id={doc_id}"
        resp_download = requests.get(
            url_download,
            headers=HEADERS,
            timeout=TIMEOUT_DOWNLOAD
        )
        resp_download.raise_for_status()

        # Decodificar XML (vem em Base64)
        xml_content = base64.b64decode(resp_download.content)

        # Salvar temporariamente (para debug se necessário)
        nome_arquivo_temp = f'temp_{cnpj_alvo}_{doc_id}.xml'
        with open(nome_arquivo_temp, 'wb') as f:
            f.write(xml_content)

        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 3: EXTRACT - Extrair todos os dados do XML
        # ─────────────────────────────────────────────────────────────────────

        dados_extraidos = extrair_dados_xml_completo(xml_content)

        # Adicionar metadados do processo
        dados_extraidos['NOME_FUNDO'] = nome_fundo
        dados_extraidos['STATUS'] = 'SUCESSO'
        dados_extraidos['ID_DOCUMENTO'] = doc_id
        dados_extraidos['DATA_REFERENCIA_DOC'] = data_referencia
        dados_extraidos['MENSAGEM_ERRO'] = None

        return dados_extraidos

    except requests.exceptions.HTTPError as e:
        return {
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo,
            'STATUS': 'ERRO_HTTP',
            'MENSAGEM_ERRO': f'HTTP Error: {str(e)}'
        }

    except requests.exceptions.Timeout as e:
        return {
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo,
            'STATUS': 'TIMEOUT',
            'MENSAGEM_ERRO': f'Timeout na requisição: {str(e)}'
        }

    except requests.exceptions.RequestException as e:
        return {
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo,
            'STATUS': 'ERRO_CONEXAO',
            'MENSAGEM_ERRO': f'Erro de conexão: {str(e)}'
        }

    except ET.ParseError as e:
        return {
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo,
            'STATUS': 'ERRO_PARSE_XML',
            'MENSAGEM_ERRO': f'Erro ao processar XML: {str(e)}'
        }

    except Exception as e:
        return {
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo,
            'STATUS': 'ERRO_INESPERADO',
            'MENSAGEM_ERRO': f'Erro inesperado: {str(e)}'
        }

    finally:
        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 4: CLEANUP - Remover arquivo temporário
        # ─────────────────────────────────────────────────────────────────────
        if nome_arquivo_temp and os.path.exists(nome_arquivo_temp):
            try:
                os.remove(nome_arquivo_temp)
            except OSError:
                pass  # Ignora erro na remoção


print("✅ Função ETL completa definida!")
print("   • Discovery → Download → Parse → Extract → Cleanup")
print("   • Tratamento completo de erros")
print("   • Cleanup automático garantido")

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 5: CARREGAMENTO DOS CNPJs DO CSV
# ═══════════════════════════════════════════════════════════════════════════════

# Nome do arquivo CSV de entrada (faça upload deste arquivo no Colab)
ARQUIVO_CSV_ENTRADA = 'lista_cnpjs_fidc.csv'

try:
    # Carregar CSV com lista de CNPJs
    df_cnpjs_entrada = pd.read_csv(ARQUIVO_CSV_ENTRADA, encoding='utf-8-sig')

    print(f"✅ Arquivo '{ARQUIVO_CSV_ENTRADA}' carregado com sucesso!")
    print(f"   • Total de CNPJs: {len(df_cnpjs_entrada)}")

    # Converter CNPJ para string (remove notação científica)
    df_cnpjs_entrada['CNPJ'] = df_cnpjs_entrada['CNPJ'].astype(str).str.replace('.0', '', regex=False)

    # Garantir que CNPJ tem 14 dígitos
    df_cnpjs_entrada['CNPJ'] = df_cnpjs_entrada['CNPJ'].str.zfill(14)

    print(f"\n📋 Primeiras 5 linhas:")
    display(df_cnpjs_entrada.head())

    print(f"\n⏱️  Tempo estimado de processamento:")
    tempo_estimado = len(df_cnpjs_entrada) * DELAY_ENTRE_REQUISICOES / 60
    print(f"   • {tempo_estimado:.1f} minutos (com delay de {DELAY_ENTRE_REQUISICOES}s)")

except FileNotFoundError:
    print(f"❌ ERRO: Arquivo '{ARQUIVO_CSV_ENTRADA}' não encontrado!")
    print(f"\n📝 Instruções:")
    print(f"   1. Faça upload do arquivo '{ARQUIVO_CSV_ENTRADA}' no Colab")
    print(f"   2. Execute esta célula novamente")
    print(f"\nPara fazer upload: Use o ícone de pasta (📁) na barra lateral esquerda")
    raise

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 6: PROCESSAMENTO EM LOTE
# ═══════════════════════════════════════════════════════════════════════════════

print(f"\n{'═'*80}")
print(f"🚀 INICIANDO PROCESSAMENTO DE {len(df_cnpjs_entrada)} CNPJs")
print(f"{'═'*80}\n")

resultados_consolidados = []

for i, row in df_cnpjs_entrada.iterrows():
    cnpj = row['CNPJ']
    nome_fundo = row.get('NOME_FUNDO', '')

    # Truncar nome para log (máx 50 caracteres)
    nome_display = nome_fundo[:47] + "..." if len(nome_fundo) > 50 else nome_fundo

    print(f"[{i+1}/{len(df_cnpjs_entrada)}] {cnpj} - {nome_display}... ", end="")

    # Executar ETL
    resultado = etl_fidc_completo(cnpj, nome_fundo)
    resultados_consolidados.append(resultado)

    # Log do resultado
    if resultado.get('STATUS') == 'SUCESSO':
        data_ref = resultado.get('DATA_REFERENCIA_DOC', 'N/A')
        ativo = resultado.get('ATIVO_TOTAL', 0)
        npl = resultado.get('INDICE_NPL_PERCENTUAL', 0)
        print(f"✅ OK (Ref: {data_ref}, NPL: {npl:.2f}%)")
    else:
        status = resultado.get('STATUS', 'ERRO')
        print(f"❌ {status}")

    # Delay entre requisições (evita bloqueio da API)
    if i < len(df_cnpjs_entrada) - 1:  # Não espera no último
        time.sleep(DELAY_ENTRE_REQUISICOES)

    # Checkpoint a cada 50 CNPJs (salva progresso intermediário)
    if (i + 1) % 50 == 0:
        df_temp = pd.DataFrame(resultados_consolidados)
        df_temp.to_csv('checkpoint_temp.csv', index=False, encoding='utf-8-sig', sep=';', decimal=',')
        print(f"   💾 Checkpoint salvo ({i+1} CNPJs processados)")

print(f"\n{'═'*80}")
print(f"✅ PROCESSAMENTO CONCLUÍDO!")
print(f"{'═'*80}\n")

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 7: CONSOLIDAÇÃO E ESTATÍSTICAS
# ═══════════════════════════════════════════════════════════════════════════════

# Criar DataFrame final
df_resultado_final = pd.DataFrame(resultados_consolidados)

# Estatísticas de processamento
total_sucesso = len(df_resultado_final[df_resultado_final['STATUS'] == 'SUCESSO'])
total_erros = len(df_resultado_final[df_resultado_final['STATUS'] != 'SUCESSO'])

print(f"📊 ESTATÍSTICAS DO PROCESSAMENTO")
print(f"{'─'*80}")
print(f"   Total de CNPJs processados: {len(df_cnpjs_entrada)}")
print(f"   ✅ Sucesso: {total_sucesso} ({total_sucesso/len(df_cnpjs_entrada)*100:.1f}%)")
print(f"   ❌ Erros: {total_erros} ({total_erros/len(df_cnpjs_entrada)*100:.1f}%)")

# Distribuição de erros
if total_erros > 0:
    print(f"\n⚠️  TIPOS DE ERROS:")
    erros_por_tipo = df_resultado_final[df_resultado_final['STATUS'] != 'SUCESSO']['STATUS'].value_counts()
    for status, count in erros_por_tipo.items():
        print(f"   • {status}: {count} ({count/total_erros*100:.1f}%)")

# Estatísticas dos dados extraídos (apenas sucessos)
df_sucesso = df_resultado_final[df_resultado_final['STATUS'] == 'SUCESSO'].copy()

if not df_sucesso.empty:
    print(f"\n💰 ESTATÍSTICAS FINANCEIRAS (Fundos com sucesso)")
    print(f"{'─'*80}")

    ativo_total_soma = df_sucesso['ATIVO_TOTAL'].sum()
    ativo_total_media = df_sucesso['ATIVO_TOTAL'].mean()
    ativo_total_mediana = df_sucesso['ATIVO_TOTAL'].median()

    inadimpl_soma = df_sucesso['INADIMPLENCIA_TOTAL'].sum()
    npl_medio = df_sucesso['INDICE_NPL_PERCENTUAL'].mean()
    npl_mediano = df_sucesso['INDICE_NPL_PERCENTUAL'].median()
    npl_max = df_sucesso['INDICE_NPL_PERCENTUAL'].max()

    print(f"   Ativo Total (Soma):           R$ {ativo_total_soma:,.2f}")
    print(f"   Ativo Total (Média):          R$ {ativo_total_media:,.2f}")
    print(f"   Ativo Total (Mediana):        R$ {ativo_total_mediana:,.2f}")
    print(f"   Inadimplência (Soma):         R$ {inadimpl_soma:,.2f}")
    print(f"   NPL Médio:                    {npl_medio:.2f}%")
    print(f"   NPL Mediano:                  {npl_mediano:.2f}%")
    print(f"   NPL Máximo:                   {npl_max:.2f}%")

    # Fundos com maior NPL
    df_npl_alto = df_sucesso[df_sucesso['INDICE_NPL_PERCENTUAL'] > 5.0].sort_values('INDICE_NPL_PERCENTUAL', ascending=False)

    if not df_npl_alto.empty:
        print(f"\n⚠️  TOP 10 FUNDOS COM MAIOR NPL (> 5%)")
        print(f"{'─'*80}")
        for idx, row in df_npl_alto.head(10).iterrows():
            nome = row['NOME_FUNDO'][:45] + "..." if len(row['NOME_FUNDO']) > 48 else row['NOME_FUNDO']
            cnpj = row['CNPJ_FUNDO']
            npl = row['INDICE_NPL_PERCENTUAL']
            ativo = row['ATIVO_TOTAL']
            print(f"   • {nome}")
            print(f"     CNPJ: {cnpj} | NPL: {npl:.2f}% | Ativo: R$ {ativo:,.2f}")

# Preview do DataFrame
print(f"\n📋 PREVIEW DOS DADOS (Primeiras 10 linhas)")
print(f"{'─'*80}")
colunas_preview = ['CNPJ_FUNDO', 'NOME_FUNDO', 'STATUS', 'DATA_REFERENCIA_DOC',
                   'ATIVO_TOTAL', 'INADIMPLENCIA_TOTAL', 'INDICE_NPL_PERCENTUAL']
display(df_resultado_final[colunas_preview].head(10))

# ═══════════════════════════════════════════════════════════════════════════════
# CÉLULA 8: EXPORTAÇÃO DOS DADOS
# ═══════════════════════════════════════════════════════════════════════════════

# Exportar para CSV (formato universal)
nome_arquivo_csv = 'base_fidc_completa.csv'
df_resultado_final.to_csv(nome_arquivo_csv, index=False, encoding='utf-8-sig', sep=';', decimal=',')

print(f"\n💾 EXPORTAÇÃO DOS DADOS")
print(f"{'─'*80}")
print(f"   ✅ CSV salvo: {nome_arquivo_csv}")
print(f"      • Encoding: UTF-8 com BOM (compatível com Excel)")
print(f"      • Separador: ponto-e-vírgula (;)")
print(f"      • Decimal: vírgula (,)")
print(f"      • Linhas: {len(df_resultado_final)}")
print(f"      • Colunas: {len(df_resultado_final.columns)}")

# Exportar apenas fundos com sucesso (opcional)
if total_sucesso > 0:
    nome_arquivo_sucesso = 'base_fidc_sucesso.csv'
    df_sucesso.to_csv(nome_arquivo_sucesso, index=False, encoding='utf-8-sig', sep=';', decimal=',')
    print(f"   ✅ CSV (só sucessos): {nome_arquivo_sucesso}")
    print(f"      • Linhas: {len(df_sucesso)}")

# Exportar fundos com erro (para análise)
if total_erros > 0:
    df_erros = df_resultado_final[df_resultado_final['STATUS'] != 'SUCESSO']
    nome_arquivo_erros = 'base_fidc_erros.csv'
    df_erros.to_csv(nome_arquivo_erros, index=False, encoding='utf-8-sig', sep=';', decimal=',')
    print(f"   ⚠️  CSV (erros): {nome_arquivo_erros}")
    print(f"      • Linhas: {len(df_erros)}")

# Exportar para Excel (se necessário)
try:
    nome_arquivo_excel = 'base_fidc_completa.xlsx'

    # Criar Excel com múltiplas abas
    with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:
        df_resultado_final.to_excel(writer, sheet_name='Todos', index=False)

        if total_sucesso > 0:
            df_sucesso.to_excel(writer, sheet_name='Sucesso', index=False)

        if total_erros > 0:
            df_erros.to_excel(writer, sheet_name='Erros', index=False)

    print(f"   ✅ Excel salvo: {nome_arquivo_excel}")
    print(f"      • Abas: Todos ({len(df_resultado_final)}), Sucesso ({total_sucesso}), Erros ({total_erros})")

except ImportError:
    print(f"   ⚠️  Excel não gerado (instale: !pip install openpyxl)")

print(f"\n{'═'*80}")
print(f"🎉 ETL CONCLUÍDO COM SUCESSO!")
print(f"{'═'*80}")
print(f"\n📥 Para baixar os arquivos:")
print(f"   1. Clique no ícone de pasta (📁) na barra lateral esquerda")
print(f"   2. Localize os arquivos CSV/Excel gerados")
print(f"   3. Clique nos 3 pontinhos (...) ao lado do arquivo")
print(f"   4. Selecione 'Download'")
print(f"\n🔄 Próximos passos:")
print(f"   1. Importe '{nome_arquivo_csv}' no Power BI / Tableau / Excel")
print(f"   2. Crie dashboards com os 90+ campos disponíveis")
print(f"   3. Analise fundos com alto NPL")
print(f"   4. Identifique tendências de inadimplência por segmento")

# ═════════════════════════════════════════════════════════════════════
# ETL FIDC V3 - EXTRAÇÃO COMPLETA DE DADOS FINANCEIROS (90+ CAMPOS)
# ═════════════════════════════════════════════════════════════════════
# Autor: Rafael Augusto
# Data: Dezembro 2025
# Objetivo: Extrair dados completos de FIDCs via API B3 para análise em BI
# Input: lista_cnpjs_fidc.csv (441 CNPJs únicos)
# Output: base_fidc_completa.csv (90+ campos por CNPJ)
# ═════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 1: IMPORTS E CONFIGURAÇÕES
# ═════════════════════════════════════════════════════════════════════

import pandas as pd
import requests
import base64
import xml.etree.ElementTree as ET
import time
import os
from typing import Dict, Optional

# Configurações da API B3
URL_API_BUSCA = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
URL_API_DOWNLOAD = "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Configurações de controle
DELAY_ENTRE_REQUISICOES = 2  # segundos (recomendado: 2-3s para evitar bloqueio)
TIMEOUT_BUSCA = 10  # segundos
TIMEOUT_DOWNLOAD = 20  # segundos

print("✅ Bibliotecas importadas com sucesso!")
print(f"📊 Configurações:")
print(f"   • Delay entre requisições: {DELAY_ENTRE_REQUISICOES}s")
print(f"   • Timeout busca: {TIMEOUT_BUSCA}s")
print(f"   • Timeout download: {TIMEOUT_DOWNLOAD}s")

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 2: FUNÇÕES AUXILIARES
# ═════════════════════════════════════════════════════════════════════

def limpar_tag(tag: str) -> str:
    """
    Remove namespace XML das tags.

    Exemplo:
        {urn:fidc}VL_DISPONIB → VL_DISPONIB
    """
    return tag.split('}')[-1] if '}' in tag else tag


def converter_valor(texto: str) -> float:
    """
    Converte string no formato brasileiro para float.

    Exemplos:
        "1.234.567,89" → 1234567.89
        "123,45"       → 123.45
        ""             → 0.0
        None           → 0.0
    """
    if not texto or not str(texto).strip():
        return 0.0

    try:
        # Remove pontos (separador de milhar) e troca vírgula por ponto
        texto_limpo = str(texto).replace('.', '').replace(',', '.')
        return float(texto_limpo)
    except (ValueError, AttributeError):
        return 0.0


def buscar_valor_xml(root: ET.Element, caminho: str) -> any:
    """
    Busca um valor no XML e tenta converter para float.
    Se não conseguir, retorna como string.

    Args:
        root: Elemento raiz do XML
        caminho: Caminho XPath simplificado (ex: "CRED_EXISTE/VL_SOM_DICRED_AQUIS")

    Returns:
        float ou string ou 0.0/''
    """
    elemento = root.find(f'.//{caminho}')

    if elemento is not None and elemento.text:
        # Tentar converter para float (valores monetários)
        try:
            return converter_valor(elemento.text)
        except:
            # Se falhar, retornar como string (datas, textos, etc)
            return elemento.text.strip()

    # Retornar 0.0 para campos numéricos, '' para texto
    return 0.0 if '/' in caminho else ''


print("✅ Funções auxiliares definidas:")
print("   • limpar_tag()")
print("   • converter_valor()")
print("   • buscar_valor_xml()")

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 3: FUNÇÃO DE EXTRAÇÃO COMPLETA DO XML (90+ CAMPOS)
# ═════════════════════════════════════════════════════════════════════

def extrair_dados_xml_completo(xml_content: bytes) -> Dict:
    """
    Extrai TODOS os campos relevantes do XML de Informe Mensal FIDC.

    Args:
        xml_content: Conteúdo XML em bytes (já decodificado do Base64)

    Returns:
        Dicionário com 90+ campos estruturados para análise
    """
    root = ET.fromstring(xml_content)

    # ─────────────────────────────────────────────────────────────────────────
    # EXTRAÇÃO DE DADOS POR SEÇÃO
    # ─────────────────────────────────────────────────────────────────────────

    dados = {
        # ═══ SEÇÃO 1: IDENTIFICAÇÃO DO FUNDO ═══
        'CNPJ_FUNDO': buscar_valor_xml(root, 'NR_CNPJ_FUNDO'),
        'CNPJ_ADMINISTRADOR': buscar_valor_xml(root, 'NR_CNPJ_ADM'),
        'DATA_COMPETENCIA': buscar_valor_xml(root, 'DT_COMPT'),
        'TIPO_CONDOMINIO': buscar_valor_xml(root, 'TP_CONDOMINIO'),
        'FUNDO_EXCLUSIVO': buscar_valor_xml(root, 'FDO_EXCL'),
        'CLASSE_UNICA': buscar_valor_xml(root, 'CLASS_UNICA'),
        'COTISTA_VINCULADO': buscar_valor_xml(root, 'COTST_VINCUL'),

        # ═══ SEÇÃO 2: ATIVOS GERAIS ═══
        'ATIVO_TOTAL': buscar_valor_xml(root, 'VL_SOM_APLIC_ATIVO'),
        'DISPONIBILIDADES': buscar_valor_xml(root, 'VL_DISPONIB'),
        'CARTEIRA_TOTAL': buscar_valor_xml(root, 'VL_CARTEIRA'),
        'OUTROS_ATIVOS_TOTAL': buscar_valor_xml(root, 'VL_SOM_OUTROS_ATIVOS'),
        'OUTROS_ATIVOS_CURTO_PRAZO': buscar_valor_xml(root, 'OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_CURPRZ'),
        'OUTROS_ATIVOS_LONGO_PRAZO': buscar_valor_xml(root, 'OUTROS_ATIVOS/VL_OUTRO_VL_RECEB_LPRAZO'),

        # ═══ SEÇÃO 3: CRÉDITOS EXISTENTES (Principal fonte de inadimplência) ═══
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

        # ═══ SEÇÃO 4: DIREITOS CREDITÓRIOS (DICRED) ═══
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

        # ═══ SEÇÃO 5: VALORES MOBILIÁRIOS ═══
        'VALORES_MOBILIARIOS_TOTAL': buscar_valor_xml(root, 'VALORES_MOB/VL_SOM_VALORES_MOB'),
        'DEBENTURES': buscar_valor_xml(root, 'VALORES_MOB/VL_DEBT'),
        'CRI': buscar_valor_xml(root, 'VALORES_MOB/VL_CRI'),
        'NOTAS_PROMISSORIAS_COMERCIAIS': buscar_valor_xml(root, 'VALORES_MOB/VL_NP_COMERC'),
        'LETRAS_FINANCEIRAS': buscar_valor_xml(root, 'VALORES_MOB/VL_LETRA_FINANC'),
        'COTAS_FIF': buscar_valor_xml(root, 'VALORES_MOB/VL_CLS_COTA_FIF'),
        'OUTROS_DIREITOS_CREDITORIOS': buscar_valor_xml(root, 'VALORES_MOB/VL_OUTRO_DICRED'),

        # ═══ SEÇÃO 6: OUTROS ATIVOS FINANCEIROS ═══
        'TITULOS_PUBLICOS_FEDERAIS': buscar_valor_xml(root, 'VL_TITPUB_FED'),
        'CDB': buscar_valor_xml(root, 'VL_CDB'),
        'APLICACOES_COMPROMISSADAS': buscar_valor_xml(root, 'VL_APLIC_OPER_COMPSS'),
        'ATIVOS_FINANCEIROS_RF': buscar_valor_xml(root, 'VL_ATIV_FINANC_RF'),
        'COTAS_FIDC': buscar_valor_xml(root, 'VL_COTA_FIDC'),

        # ═══ SEÇÃO 7: MERCADO DE DERIVATIVOS ═══
        'DERIVATIVOS_TOTAL': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_SOM_MERC_DERIVATIVO'),
        'TERMO_COMPRADOR': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_TERMO_POS_COMPRD'),
        'OPCOES_TITULAR': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_OP_POS_TITUL'),
        'FUTUROS_AJUSTE_POSITIVO': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_MERC_FUT_AJUST_POSIT'),
        'SWAP_A_RECEBER': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_DIFER_SWAP_RECEB'),
        'COBERTURA_PRESTADA': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_COBERT_PREST'),
        'DEPOSITOS_MARGEM': buscar_valor_xml(root, 'MERC_DERIVATIVO/VL_DEPOS_MARGEM'),

        # ═══ SEÇÃO 8: SEGMENTAÇÃO DA CARTEIRA ═══
        'CARTEIRA_SEGMENTADA_TOTAL': buscar_valor_xml(root, 'CART_SEGMT/VL_SOM_CART_SEGMT'),
        'SEGMT_INDUSTRIAL': buscar_valor_xml(root, 'CART_SEGMT/VL_IND'),
        'SEGMT_MERCADO_IMOBILIARIO': buscar_valor_xml(root, 'CART_SEGMT/VL_MERC_IMOBIL'),
        'SEGMT_AGRONEGOCIO': buscar_valor_xml(root, 'CART_SEGMT/VL_AGRONEG'),
        'SEGMT_CARTAO_CREDITO': buscar_valor_xml(root, 'CART_SEGMT/VL_CART_CRED'),
        'SEGMT_ACAO_JUDICIAL': buscar_valor_xml(root, 'CART_SEGMT/VL_ACAO_JUDIC'),
        'SEGMT_PROPRIEDADE_INTELECTUAL': buscar_valor_xml(root, 'CART_SEGMT/VL_PROPRD_MARCA_PATENT'),

        # ─── Subsegmento: COMERCIAL ───
        'SEGMT_COMERCIAL_TOTAL': buscar_valor_xml(root, 'SEGMT_COMERC/VL_SOM_SEGMT_COMERC'),
        'SEGMT_COMERCIO': buscar_valor_xml(root, 'SEGMT_COMERC/VL_COMERC'),
        'SEGMT_COMERCIO_VAREJO': buscar_valor_xml(root, 'SEGMT_COMERC/VL_COMERC_VARJ'),
        'SEGMT_ARREND_MERCANTIL': buscar_valor_xml(root, 'SEGMT_COMERC/VL_ARREND_MERCNT'),

        # ─── Subsegmento: SERVIÇOS ───
        'SEGMT_SERVICOS_TOTAL': buscar_valor_xml(root, 'SEGMT_SERV/VL_SOM_SEGMT_SERV'),
        'SEGMT_SERVICOS_GERAIS': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV'),
        'SEGMT_SERVICOS_PUBLICOS': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_PUBLIC'),
        'SEGMT_SERVICOS_EDUCACAO': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_EDUC'),
        'SEGMT_SERVICOS_ENTRETENIMENTO': buscar_valor_xml(root, 'SEGMT_SERV/VL_SERV_ENTRETEN'),

        # ─── Subsegmento: FINANCEIRO ───
        'SEGMT_FINANCEIRO_TOTAL': buscar_valor_xml(root, 'SEGMT_FINANC/VL_SOM_SEGMT_FINANC'),
        'SEGMT_FINANC_CREDITO_PESSOA': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_PESSOA'),
        'SEGMT_FINANC_CONSIGNADO': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_PESSOA_CONSIG'),
        'SEGMT_FINANC_CORPORATIVO': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_CRED_CORPOR'),
        'SEGMT_FINANC_MIDDLE_MARKET': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_MMARKET'),
        'SEGMT_FINANC_VEICULOS': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_VEICL'),
        'SEGMT_FINANC_IMOB_EMPRESARIAL': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_IMOBIL_EMPSRL'),
        'SEGMT_FINANC_IMOB_RESIDENCIAL': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_IMOBIL_RESID'),
        'SEGMT_FINANC_OUTROS': buscar_valor_xml(root, 'SEGMT_FINANC/VL_FINANC_OUTRO'),

        # ─── Subsegmento: FACTORING ───
        'SEGMT_FACTORING_TOTAL': buscar_valor_xml(root, 'SEGMT_FACT/VL_SOM_SEGMT_FACT'),
        'SEGMT_FACTORING_PESSOA': buscar_valor_xml(root, 'SEGMT_FACT/VL_FACT_PESSOA'),
        'SEGMT_FACTORING_CORPORATIVO': buscar_valor_xml(root, 'SEGMT_FACT/VL_FACT_CORPOR'),

        # ─── Subsegmento: SETOR PÚBLICO ───
        'SEGMT_SETOR_PUBLICO_TOTAL': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SOM_SEGMT_SETOR_PUBLIC'),
        'SEGMT_PRECATORIOS': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_PRECAT'),
        'SEGMT_CREDITOS_TRIBUTARIOS': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_CRED_TRIBUT'),
        'SEGMT_ROYALTIES': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_ROYA'),
        'SEGMT_SETOR_PUBLICO_OUTROS': buscar_valor_xml(root, 'SEGMT_SETOR_PUBLIC/VL_SETOR_PUBLIC_OUTRO'),
    }

    # ─────────────────────────────────────────────────────────────────────────
    # CÁLCULO DE INDICADORES CRÍTICOS
    # ─────────────────────────────────────────────────────────────────────────

    # Inadimplência consolidada (prioriza maior valor entre CRED e DICRED)
    inadimpl_cred = dados['CRED_INADIMPLENCIA']
    inadimpl_dicred = dados['DICRED_INADIMPLENCIA']
    dados['INADIMPLENCIA_TOTAL'] = max(inadimpl_cred, inadimpl_dicred)

    # Carteira de crédito para cálculo de NPL
    carteira_credito = dados['CREDITOS_ADQUIRIDOS']

    # ### NOVO: Verifica se 'DATA_COMPETENCIA' é uma string válida para conversão
    if isinstance(dados['DATA_COMPETENCIA'], str):
        try:
            # Converte para formato YYYY-MM-DD para garantir ordenação correta
            # Assume que a data está no formato MM/YYYY
            month, year = map(int, dados['DATA_COMPETENCIA'].split('/'))
            dados['DATA_COMPETENCIA_ORDENAVEL'] = f"{year:04d}-{month:02d}-01"
        except (ValueError, AttributeError):
            dados['DATA_COMPETENCIA_ORDENAVEL'] = None # Ou manter o valor original
    else:
        dados['DATA_COMPETENCIA_ORDENAVEL'] = None

    # ###

    # Índice de NPL (Non-Performing Loans) - CORRIGIDO
    if carteira_credito > 0 and dados['INADIMPLENCIA_TOTAL'] > 0:
        dados['INDICE_NPL_PERCENTUAL'] = (dados['INADIMPLENCIA_TOTAL'] / carteira_credito) * 100
    else:
        dados['INDICE_NPL_PERCENTUAL'] = 0.0

    # Taxa de liquidez imediata (Disponibilidades / Ativo Total)
    if dados['ATIVO_TOTAL'] > 0:
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = (dados['DISPONIBILIDADES'] / dados['ATIVO_TOTAL']) * 100
    else:
        dados['TAXA_LIQUIDEZ_PERCENTUAL'] = 0.0

    # Concentração em crédito (Carteira / Ativo Total)
    if dados['ATIVO_TOTAL'] > 0:
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = (carteira_credito / dados['ATIVO_TOTAL']) * 100
    else:
        dados['CONCENTRACAO_CREDITO_PERCENTUAL'] = 0.0

    return dados


print("✅ Função de extração completa definida:")
print("   • 90+ campos extraídos")
print("   • 4 indicadores calculados automaticamente")
print("   • Tratamento robusto de valores ausentes")

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 4: FUNÇÃO ETL COMPLETA
# ═════════════════════════════════════════════════════════════════════

def etl_fidc_completo(cnpj_alvo: str, nome_fundo_referencia: str = "") -> List[Dict]:
    """
    Executa o pipeline ETL completo para um CNPJ, lidando com múltiplos fundos
    associados e extraindo o informe mensal mais recente para cada um.

    Workflow:
        1. Discovery: Busca documentos do fundo na API B3 com limite maior.
        2. Filter: Filtra estritamente por 'Informe Mensal Estruturado'.
        3. Deduplicate: Seleciona o informe mais recente para cada fundo único.
        4. Download: Baixa o XML de cada informe selecionado.
        5. Parse & Extract: Decodifica e processa o XML, extraindo 90+ campos.
        6. Cleanup: Remove arquivos temporários.

    Args:
        cnpj_alvo: CNPJ do fundo (string com 14 dígitos)
        nome_fundo_referencia: Nome do fundo (opcional, para logs e possível filtragem inicial)

    Returns:
        Uma lista de dicionários, onde cada dicionário contém os dados extraídos
        de um fundo único para o CNPJ alvo. Retorna uma lista vazia se nenhum
        documento válido for encontrado ou em caso de erro.
    """

    resultados_fundo_cnpj = []

    try:
        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 1: DISCOVERY - Buscar documentos disponíveis
        # ─────────────────────────────────────────────────────────────────────

        params = {
            'd': 0,         # Documento
            's': 0,         # Start (paginação)
            'l': 200,       # Limit (aumentado para 200)
            'cnpjFundo': cnpj_alvo
        }

        resp_busca = requests.get(
            URL_API_BUSCA,
            params=params,
            headers=HEADERS,
            timeout=TIMEOUT_BUSCA
        )
        resp_busca.raise_for_status()

        data = resp_busca.json().get('data', [])

        if not data:
            # print(f"DEBUG: etl_fidc_completo para {cnpj_alvo}: NENHUM DADO DA API. Retornando lista vazia.")
            return []

        df_docs = pd.DataFrame(data)

        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 2: FILTER - Filtrar estritamente por 'Informe Mensal Estruturado'
        # ─────────────────────────────────────────────────────────────────────

        # Filtrar apenas 'Informe Mensal Estruturado' e status 'Ativo'
        # Adicionado .str.strip() para lidar com espaços em branco inesperados
        df_mensal = df_docs[
            (df_docs['tipoDocumento'].str.strip() == "Informe Mensal Estruturado") &
            (df_docs['situacaoDocumento'].str.strip() == "A")
        ].copy()

        if df_mensal.empty:
            # print(f"DEBUG: etl_fidc_completo para {cnpj_alvo}: df_mensal vazio após filtro tipo/status. Retornando lista vazia.")
            return []

        # ─────────────────────────────────────────────────────────────────────
        # ETAPA 3: DEDUPLICATE - Selecionar o mais recente para cada fundo único
        # ─────────────────────────────────────────────────────────────────────

        # Converte 'dataReferencia' para um formato ordenável (MM/YYYY -> YYYY-MM)
        # Adiciona '01' para criar uma data completa, facilitando a conversão para datetime
        df_mensal['dataReferenciaOrdenavel'] = pd.to_datetime(
            df_mensal['dataReferencia'].str[3:] + '-' + df_mensal['dataReferencia'].str[:2] + '-01',
            format='%Y-%m-%d', errors='coerce'
        )

        # Remove linhas com dataReferencia inválida (NaT)
        df_mensal.dropna(subset=['dataReferenciaOrdenavel'], inplace=True)

        if df_mensal.empty:
            # print(f"DEBUG: etl_fidc_completo para {cnpj_alvo}: df_mensal vazio após dropna de datas. Retornando lista vazia.")
            return []

        # DEBUG PRINT: Mostrar df_mensal antes da deduplicação para o CNPJ de interesse
        # if cnpj_alvo == '51199121000145':
            # print(f"DEBUG: df_mensal para {cnpj_alvo} antes da deduplicação:\n{df_mensal[['id', 'dataReferencia', 'dataReferenciaOrdenavel', 'descricaoFundo']].to_string()}")


        # Ordena por nome do fundo e data de referência (mais recente primeiro)
        df_mensal.sort_values(by=['descricaoFundo', 'dataReferenciaOrdenavel'], ascending=[True, False], inplace=True)

        # Pega o documento mais recente para cada fundo único (baseado em descricaoFundo)
        df_fundos_unicos = df_mensal.groupby('descricaoFundo').first().reset_index()
        # print(f"DEBUG: etl_fidc_completo para {cnpj_alvo}: Encontrado(s) {len(df_fundos_unicos)} fundo(s) único(s) após deduplicação.")

        # Itera sobre cada fundo único encontrado
        for index, doc_info in df_fundos_unicos.iterrows():
            doc_id = doc_info['id']
            data_referencia = doc_info.get('dataReferencia', 'N/A')
            nome_fundo_doc = doc_info.get('descricaoFundo', 'Nome N/A')

            # ────────────────────────────────────────────────────────────────────
            # ETAPA 4: DOWNLOAD - Baixar XML do documento
            # ────────────────────────────────────────────────────────────────────
            nome_arquivo_temp = None # Reinicia para cada iteração
            try:
                url_download = f"{URL_API_DOWNLOAD}?id={doc_id}"
                resp_download = requests.get(
                    url_download,
                    headers=HEADERS,
                    timeout=TIMEOUT_DOWNLOAD
                )
                resp_download.raise_for_status()

                # Decodificar XML (vem em Base64)
                xml_content = base64.b64decode(resp_download.content)

                # Salvar temporariamente
                nome_arquivo_temp = f'temp_{cnpj_alvo}_{doc_id}.xml'
                with open(nome_arquivo_temp, 'wb') as f:
                    f.write(xml_content)

                # ─────────────────────────────────────────────────────────────────
                # ETAPA 5: EXTRACT - Extrair todos os dados do XML
                # ─────────────────────────────────────────────────────────────────

                dados_extraidos = extrair_dados_xml_completo(xml_content)

                # Garante que CNPJ_FUNDO usa cnpj_alvo como fallback
                if not dados_extraidos.get('CNPJ_FUNDO'):
                    dados_extraidos['CNPJ_FUNDO'] = cnpj_alvo

                # Adicionar metadados do processo
                dados_extraidos['NOME_FUNDO'] = nome_fundo_doc # Usa o nome do fundo do documento
                dados_extraidos['STATUS'] = 'SUCESSO'
                dados_extraidos['ID_DOCUMENTO'] = doc_id
                dados_extraidos['DATA_REFERENCIA_DOC'] = data_referencia
                dados_extraidos['MENSAGEM_ERRO'] = None

                resultados_fundo_cnpj.append(dados_extraidos)

            except requests.exceptions.HTTPError as e:
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_HTTP_DOWNLOAD',
                    'MENSAGEM_ERRO': f'HTTP Error no download do doc {doc_id}: {str(e)}'
                })
            except requests.exceptions.Timeout as e:
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'TIMEOUT_DOWNLOAD',
                    'MENSAGEM_ERRO': f'Timeout no download do doc {doc_id}: {str(e)}'
                })
            except requests.exceptions.RequestException as e:
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_CONEXAO_DOWNLOAD',
                    'MENSAGEM_ERRO': f"Erro de conexão no download do doc {doc_id}: {str(e)}"
                })
            except ET.ParseError as e:
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_PARSE_XML',
                    'MENSAGEM_ERRO': f'Erro ao processar XML do doc {doc_id}: {str(e)}'
                })
            except Exception as e:
                resultados_fundo_cnpj.append({
                    'CNPJ_FUNDO': cnpj_alvo,
                    'NOME_FUNDO': nome_fundo_doc,
                    'STATUS': 'ERRO_INESPERADO_DOC',
                    'MENSAGEM_ERRO': f'Erro inesperado ao processar doc {doc_id}: {str(e)}'
                })
            finally:
                # ─────────────────────────────────────────────────────────────────
                # ETAPA 6: CLEANUP - Remover arquivo temporário
                # ─────────────────────────────────────────────────────────────────
                if nome_arquivo_temp and os.path.exists(nome_arquivo_temp):
                    try:
                        os.remove(nome_arquivo_temp)
                    except OSError:
                        pass  # Ignora erro na remoção

    except requests.exceptions.HTTPError as e:
        resultados_fundo_cnpj.append({
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo_referencia,
            'STATUS': 'ERRO_HTTP_BUSCA',
            'MENSAGEM_ERRO': f'HTTP Error na busca de documentos: {str(e)}'
        })
    except requests.exceptions.Timeout as e:
        resultados_fundo_cnpj.append({
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo_referencia,
            'STATUS': 'TIMEOUT_BUSCA',
            'MENSAGEM_ERRO': f'Timeout na busca de documentos: {str(e)}'
        })
    except requests.exceptions.RequestException as e:
        resultados_fundo_cnpj.append({
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo_referencia,
            'STATUS': 'ERRO_CONEXAO_BUSCA',
            'MENSAGEM_ERRO': f"Erro de conexão na busca de documentos: {str(e)}"
        })
    except Exception as e:
        resultados_fundo_cnpj.append({
            'CNPJ_FUNDO': cnpj_alvo,
            'NOME_FUNDO': nome_fundo_referencia,
            'STATUS': 'ERRO_INESPERADO_GERAL',
            'MENSAGEM_ERRO': f'Erro inesperado no processo ETL para {cnpj_alvo}: {str(e)}'
        })

    return resultados_fundo_cnpj


# REMOVIDO: print("✅ Função ETL completa definida!") para evitar poluir o output durante debug
# REMOVIDO: print("   • Discovery → Download → Parse → Extract → Cleanup")
# REMOVIDO: print("   • Tratamento completo de erros")
# REMOVIDO: print("   • Cleanup automático garantido")
# REMOVIDO: print("   • Suporte a múltiplos fundos por CNPJ com último informe mensal")

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 5: CARREGAMENTO DOS CNPJs DO CSV
# ═════════════════════════════════════════════════════════════════════

# Nome do arquivo CSV de entrada (faça upload deste arquivo no Colab)
ARQUIVO_CSV_ENTRADA = 'lista_cnpjs_fidc.csv'

try:
    # Carregar CSV com lista de CNPJs
    df_cnpjs_entrada = pd.read_csv(ARQUIVO_CSV_ENTRADA, encoding='utf-8-sig')

    print(f"✅ Arquivo '{ARQUIVO_CSV_ENTRADA}' carregado com sucesso!")
    print(f"   • Total de CNPJs: {len(df_cnpjs_entrada)}")

    # Converter CNPJ para string (remove notação científica)
    df_cnpjs_entrada['CNPJ'] = df_cnpjs_entrada['CNPJ'].astype(str).str.replace('.0', '', regex=False)

    # Garantir que CNPJ tem 14 dígitos
    df_cnpjs_entrada['CNPJ'] = df_cnpjs_entrada['CNPJ'].str.zfill(14)

    print(f"\n📋 Primeiras 5 linhas:")
    display(df_cnpjs_entrada.head())

    print(f"\n⏱️  Tempo estimado de processamento:")
    tempo_estimado = len(df_cnpjs_entrada) * DELAY_ENTRE_REQUISICOES / 60
    print(f"   • {tempo_estimado:.1f} minutos (com delay de {DELAY_ENTRE_REQUISICOES}s)")

except FileNotFoundError:
    print(f"❌ ERRO: Arquivo '{ARQUIVO_CSV_ENTRADA}' não encontrado!")
    print(f"\n📝 Instruções:")
    print(f"   1. Faça upload do arquivo '{ARQUIVO_CSV_ENTRADA}' no Colab")
    print(f"   2. Execute esta célula novamente")
    print(f"\nPara fazer upload: Use o ícone de pasta (📁) na barra lateral esquerda")
    raise

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 6: PROCESSAMENTO EM LOTE
# ═════════════════════════════════════════════════════════════════════

print(f"\n{'═'*80}")
print(f"🚀 INICIANDO PROCESSAMENTO DE {len(df_cnpjs_entrada)} CNPJs")
print(f"{'═'*80}\n")

resultados_consolidados = []

for i, row in df_cnpjs_entrada.iterrows():
    cnpj = row['CNPJ']
    nome_fundo_ref = row.get('NOME_FUNDO', '') # Usar o nome do CSV como referência

    # Truncar nome para log (máx 50 caracteres)
    nome_display = nome_fundo_ref[:47] + "..." if len(nome_fundo_ref) > 50 else nome_fundo_ref

    print(f"[{i+1}/{len(df_cnpjs_entrada)}] {cnpj} - {nome_display}... ", end="")

    # Executar ETL para cada CNPJ, que agora pode retornar uma lista de resultados
    resultados_atuais = etl_fidc_completo(cnpj, nome_fundo_ref)

    if resultados_atuais:
        resultados_consolidados.extend(resultados_atuais)
        # Log do primeiro resultado para simplicidade no feedback
        primeiro_resultado = resultados_atuais[0]
        if primeiro_resultado.get('STATUS') == 'SUCESSO':
            data_ref = primeiro_resultado.get('DATA_REFERENCIA_DOC', 'N/A')
            npl = primeiro_resultado.get('INDICE_NPL_PERCENTUAL', 0)
            num_fundos = len(resultados_atuais)
            print(f"✅ OK ({num_fundos} Fundo(s) - Ref: {data_ref}, NPL: {npl:.2f}%) ")
        else:
            status = primeiro_resultado.get('STATUS', 'ERRO')
            # Inclui o nome do fundo para facilitar a identificação do erro
            print(f"❌ {status} (Erro para '{primeiro_resultado.get('NOME_FUNDO', 'N/A')}')")
    else:
        # Caso a função retorne lista vazia ou erro geral na busca
        resultados_consolidados.append({
            'CNPJ_FUNDO': cnpj,
            'NOME_FUNDO': nome_fundo_ref,
            'STATUS': 'NENHUM_INFORME_VALIDO',
            'MENSAGEM_ERRO': 'Nenhum informe mensal estruturado e ativo encontrado para este CNPJ.'
        })
        print(f"❌ NENHUM_INFORME_VALIDO")

    # Delay entre requisições (evita bloqueio da API)
    if i < len(df_cnpjs_entrada) - 1:  # Não espera no último
        time.sleep(DELAY_ENTRE_REQUISICOES)

    # Checkpoint a cada 50 CNPJs (salva progresso intermediário)
    if (i + 1) % 50 == 0:
        df_temp = pd.DataFrame(resultados_consolidados)
        df_temp.to_csv('checkpoint_temp.csv', index=False, encoding='utf-8-sig', sep=';', decimal=',')
        print(f"   💾 Checkpoint salvo ({i+1} CNPJs processados)")

print(f"\n{'═'*80}")
print(f"✅ PROCESSAMENTO CONCLUÍDO!")
print(f"{'═'*80}\n")

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 7: CONSOLIDAÇÃO E ESTATÍSTICAS
# ═════════════════════════════════════════════════════════════════════

# Criar DataFrame final
df_resultado_final = pd.DataFrame(resultados_consolidados)

# Estatísticas de processamento
total_sucesso = len(df_resultado_final[df_resultado_final['STATUS'] == 'SUCESSO'])
total_erros = len(df_resultado_final[df_resultado_final['STATUS'] != 'SUCESSO'])

print(f"📊 ESTATÍSTICAS DO PROCESSAMENTO")
print(f"{'─'*80}")
print(f"   Total de CNPJs de entrada: {len(df_cnpjs_entrada)}")
print(f"   Total de Informes processados (incluindo erros): {len(df_resultado_final)}")
print(f"   ✅ Sucesso: {total_sucesso} ({total_sucesso/len(df_resultado_final)*100:.1f}%) [Fundos únicos com informes válidos]")
print(f"   ❌ Erros: {total_erros} ({total_erros/len(df_resultado_final)*100:.1f}%) [Informações de erro geradas]")

# Distribuição de erros
if total_erros > 0:
    print(f"\n⚠️  TIPOS DE ERROS:")
    erros_por_tipo = df_resultado_final[df_resultado_final['STATUS'] != 'SUCESSO']['STATUS'].value_counts()
    for status, count in erros_por_tipo.items():
        # Aqui também adicionamos uma verificação para evitar IndexError se houver apenas um erro de um tipo específico.
        # E garantimos que a mensagem de erro seja acessada de forma segura.
        error_message_sample = df_resultado_final[df_resultado_final['STATUS'] == status]['MENSAGEM_ERRO'].iloc[0] if not df_resultado_final[df_resultado_final['STATUS'] == status].empty else 'N/A'
        print(f"   • {status}: {count} ({count/total_erros*100:.1f}%) - '{error_message_sample}'")

# Estatísticas dos dados extraídos (apenas sucessos)
df_sucesso = df_resultado_final[df_resultado_final['STATUS'] == 'SUCESSO'].copy()

if not df_sucesso.empty:
    print(f"\n💰 ESTATÍSTICAS FINANCEIRAS (Fundos com sucesso)")
    print(f"{'─'*80}")

    ativo_total_soma = df_sucesso['ATIVO_TOTAL'].sum()
    ativo_total_media = df_sucesso['ATIVO_TOTAL'].mean()
    ativo_total_mediana = df_sucesso['ATIVO_TOTAL'].median()

    inadimpl_soma = df_sucesso['INADIMPLENCIA_TOTAL'].sum()
    npl_medio = df_sucesso['INDICE_NPL_PERCENTUAL'].mean()
    npl_mediano = df_sucesso['INDICE_NPL_PERCENTUAL'].median()
    npl_max = df_sucesso['INDICE_NPL_PERCENTUAL'].max()

    print(f"   Ativo Total (Soma):           R$ {ativo_total_soma:,.2f}")
    print(f"   Ativo Total (Média):          R$ {ativo_total_media:,.2f}")
    print(f"   Ativo Total (Mediana):        R$ {ativo_total_mediana:,.2f}")
    print(f"   Inadimplência (Soma):         R$ {inadimpl_soma:,.2f}")
    print(f"   NPL Médio:                    {npl_medio:.2f}%")
    print(f"   NPL Mediano:                  {npl_mediano:.2f}%")
    print(f"   NPL Máximo:                   {npl_max:.2f}%")

    # Fundos com maior NPL
    df_npl_alto = df_sucesso[df_sucesso['INDICE_NPL_PERCENTUAL'] > 5.0].sort_values('INDICE_NPL_PERCENTUAL', ascending=False)

    if not df_npl_alto.empty:
        print(f"\n⚠️  TOP 10 FUNDOS COM MAIOR NPL (> 5%)")
        print(f"{'─'*80}")
        for idx, row in df_npl_alto.head(10).iterrows():
            nome = row['NOME_FUNDO'][:45] + "..." if len(row['NOME_FUNDO']) > 48 else row['NOME_FUNDO']
            cnpj = row['CNPJ_FUNDO']
            npl = row['INDICE_NPL_PERCENTUAL']
            ativo = row['ATIVO_TOTAL']
            print(f"   • {nome}")
            print(f"     CNPJ: {cnpj} | NPL: {npl:.2f}% | Ativo: R$ {ativo:,.2f}")

# Preview do DataFrame
print(f"\n📋 PREVIEW DOS DADOS (Primeiras 10 linhas)")
print(f"{'─'*80}")
colunas_preview = ['CNPJ_FUNDO', 'NOME_FUNDO', 'STATUS', 'DATA_REFERENCIA_DOC',
                   'ATIVO_TOTAL', 'INADIMPLENCIA_TOTAL', 'INDICE_NPL_PERCENTUAL']
display(df_resultado_final[colunas_preview].head(10))

# ═════════════════════════════════════════════════════════════════════
# CÉLULA 8: EXPORTAÇÃO DOS DADOS
# ═════════════════════════════════════════════════════════════════════

# Exportar para CSV (formato universal)
nome_arquivo_csv = 'base_fidc_completa.csv'
df_resultado_final.to_csv(nome_arquivo_csv, index=False, encoding='utf-8-sig', sep=';', decimal=',')

print(f"\n💾 EXPORTAÇÃO DOS DATOS")
print(f"{'─'*80}")
print(f"   ✅ CSV salvo: {nome_arquivo_csv}")
print(f"      • Encoding: UTF-8 com BOM (compatível com Excel)")
print(f"      • Separador: ponto-e-vírgula (;)")
print(f"      • Decimal: vírgula (,)")
print(f"      • Linhas: {len(df_resultado_final)}")
print(f"      • Colunas: {len(df_resultado_final.columns)}")

# Exportar apenas fundos com sucesso (opcional)
if total_sucesso > 0:
    nome_arquivo_sucesso = 'base_fidc_sucesso.csv'
    df_sucesso.to_csv(nome_arquivo_sucesso, index=False, encoding='utf-8-sig', sep=';', decimal=',')
    print(f"   ✅ CSV (só sucessos): {nome_arquivo_sucesso}")
    print(f"      • Linhas: {len(df_sucesso)}")

# Exportar fundos com erro (para análise)
if total_erros > 0:
    df_erros = df_resultado_final[df_resultado_final['STATUS'] != 'SUCESSO']
    nome_arquivo_erros = 'base_fidc_erros.csv'
    df_erros.to_csv(nome_arquivo_erros, index=False, encoding='utf-8-sig', sep=';', decimal=',')
    print(f"   ⚠️  CSV (erros): {nome_arquivo_erros}")
    print(f"      • Linhas: {len(df_erros)}")

# Exportar para Excel (se necessário)
try:
    nome_arquivo_excel = 'base_fidc_completa.xlsx'

    # Criar Excel com múltiplas abas
    with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:
        df_resultado_final.to_excel(writer, sheet_name='Todos', index=False)

        if total_sucesso > 0:
            df_sucesso.to_excel(writer, sheet_name='Sucesso', index=False)

        if total_erros > 0:
            df_erros.to_excel(writer, sheet_name='Erros', index=False)

    print(f"   ✅ Excel salvo: {nome_arquivo_excel}")
    print(f"      • Abas: Todos ({len(df_resultado_final)}), Sucesso ({total_sucesso}), Erros ({total_erros})")

except ImportError:
    print(f"   ⚠️  Excel não gerado (instale: !pip install openpyxl)")

print(f"\n{'═'*80}")
print(f"🎉 ETL CONCLUÍDO COM SUCESSO!")
print(f"{'═'*80}")
print(f"\n📥 Para baixar os arquivos:")
print(f"   1. Clique no ícone de pasta (📁) na barra lateral esquerda")
print(f"   2. Localize os arquivos CSV/Excel gerados")
print(f"   3. Clique nos 3 pontinhos (...) ao lado do arquivo")
print(f"   4. Selecione 'Download'")
print(f"\n🔄 Próximos passos:")
print(f"   1. Importe '{nome_arquivo_csv}' no Power BI / Tableau / Excel")
print(f"   2. Crie dashboards com os 90+ campos disponíveis")
print(f"   3. Analise fundos com alto NPL")
print(f"   4. Identifique tendências de inadimplência por segmento")

"""
Serviço principal de ETL para FIDCs.

Orquestra todo o pipeline: busca, download, parse (com lógica notebook) e validação.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import pandas as pd
import time
import logging
from typing import Dict, List
from tqdm import tqdm

from ..extractors.api_client import B3ApiClient
from ..extractors.xml_parser import FIDCXMLParser
from ..validators.data_validator import FIDCValidator
from ..config import settings


logger = logging.getLogger(__name__)


class FIDCETLService:
    """Serviço de ETL completo para FIDCs."""
    
    def __init__(self):
        """Inicializa o serviço com componentes aprimorados."""
        self.api_client = B3ApiClient()
        self.xml_parser = FIDCXMLParser()
        self.validator = FIDCValidator()
        # IndicatorCalculator removido pois a lógica está no Parser (Notebook style)
    
    def process_single_cnpj(self, cnpj: str) -> Dict:
        """Processa um único CNPJ."""
        try:
            # 1. Buscar documentos (com Cache 24h)
            sucesso, df_docs, erro = self.api_client.buscar_documentos(cnpj)
            if not sucesso:
                return {'CNPJ_FUNDO': cnpj, 'STATUS': 'ERRO_BUSCA', 'MENSAGEM_ERRO': erro}
            
            # 2. Filtrar informe mensal
            sucesso, df_mensal, erro = self.api_client.filtrar_informe_mensal(df_docs)
            if not sucesso:
                return {'CNPJ_FUNDO': cnpj, 'STATUS': 'SEM_INFORME_MENSAL', 'MENSAGEM_ERRO': erro}
            
            # 3. Selecionar documento mais recente
            sucesso, doc, erro = self.api_client.selecionar_documento_mais_recente(df_mensal)
            if not sucesso:
                return {'CNPJ_FUNDO': cnpj, 'STATUS': 'ERRO_SELECAO', 'MENSAGEM_ERRO': erro}
            
            # 4. Baixar XML (com Cache infinito e Retry)
            sucesso, xml_content, erro = self.api_client.baixar_xml(doc['id'])
            if not sucesso:
                return {'CNPJ_FUNDO': cnpj, 'STATUS': 'ERRO_DOWNLOAD', 'MENSAGEM_ERRO': erro}
            
            # 5. Parse XML e extração (Lógica 90+ campos do Notebook)
            sucesso, fidc_data, erro = self.xml_parser.parse(xml_content, cnpj)
            if not sucesso:
                return {'CNPJ_FUNDO': cnpj, 'STATUS': 'ERRO_PARSE', 'MENSAGEM_ERRO': erro}
            
            # 6. Adicionar metadados
            dados_dict = fidc_data.to_dict()
            dados_dict['DATA_REFERENCIA_DOC'] = doc.get('dataReferencia', '')
            dados_dict['ID_DOCUMENTO'] = str(doc.get('id', ''))
            
            return dados_dict
            
        except Exception as e:
            logger.error(f"Erro inesperado CNPJ {cnpj}: {e}")
            return {'CNPJ_FUNDO': cnpj, 'STATUS': 'ERRO_INESPERADO', 'MENSAGEM_ERRO': str(e)}
    
    def process_batch(self, cnpjs: List[str], show_progress: bool = True) -> pd.DataFrame:
        """Processa lote com barra de progresso."""
        logger.info(f"Iniciando processamento de {len(cnpjs)} CNPJs")
        
        resultados = []
        iterator = tqdm(cnpjs, desc="Processando FIDCs") if show_progress else cnpjs
        
        for i, cnpj in enumerate(iterator):
            resultado = self.process_single_cnpj(cnpj)
            resultados.append(resultado)
            
            # Delay apenas se não é o último (cache hit no api_client evita delay desnecessário lá dentro?)
            # O delay deve ser respeitado para evitar bloqueio mesmo com cache se for miss
            if i < len(cnpjs) - 1:
                time.sleep(settings.DELAY_ENTRE_REQUISICOES)
        
        df = pd.DataFrame(resultados)
        
        total_sucesso = (df['STATUS'] == 'SUCESSO').sum()
        logger.info(f"Processamento concluído. Sucesso: {total_sucesso}/{len(df)}")
        return df
    
    def process_and_validate(self, cnpjs: List[str], show_progress: bool = True) -> pd.DataFrame:
        """Pipeline simplificado: ETL -> Validação (Limpeza já feita no Parser)."""
        
        # 1. ETL
        logger.info("Fase 1: ETL - Extração (Lógica Notebook)")
        df = self.process_batch(cnpjs, show_progress)
        
        # 2. Validações QA (Mantemos as validações existentes do projeto)
        logger.info("Fase 2: Validações de QA")
        if not df.empty and 'STATUS' in df.columns:
             # Só valida o que teve sucesso
             df_validado = self.validator.apply_all_validations(df)
             return df_validado
        
        return df

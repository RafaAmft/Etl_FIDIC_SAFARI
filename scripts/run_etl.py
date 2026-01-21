"""
Script principal de execução do ETL FIDC.

Executa o pipeline completo: ETL → Validações → Relatórios

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import sys
import os
from pathlib import Path
import pandas as pd
import logging

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.etl_service import FIDCETLService
from src.services.qa_service import QAService
from src.utils.logging_config import setup_logging
from src.config import settings


def main():
    """Executa o pipeline ETL completo."""
    
    # Configurar logging
    log_file = settings.OUTPUTS_DIR / "etl_fidc.log"
    setup_logging(log_level="INFO", log_file=str(log_file))
    logger = logging.getLogger(__name__)
    
    print("=" * 80)
    print("🚀 ETL FIDC - Pipeline Completo")
    print("=" * 80)
    print()
    
    # Verificar arquivo de entrada
    arquivo_entrada = settings.DATA_DIR / "lista_cnpjs_fidc.csv"
    
    if not arquivo_entrada.exists():
        logger.error(f"Arquivo não encontrado: {arquivo_entrada}")
        print(f"\n❌ ERRO: Arquivo '{arquivo_entrada}' não encontrado!")
        print(f"\nColoque o arquivo CSV com os CNPJs em: {settings.DATA_DIR}")
        return 1
    
    try:
        # 1. Carregar CNPJs
        logger.info(f"Carregando CNPJs de {arquivo_entrada}")
        df_cnpjs = pd.read_csv(arquivo_entrada)
        
        if 'CNPJ' not in df_cnpjs.columns:
            logger.error("Coluna 'CNPJ' não encontrada no CSV")
            print("\n❌ ERRO: CSV deve conter coluna 'CNPJ'")
            return 1
        
        # Converter para string e preencher com zeros
        cnpjs = df_cnpjs['CNPJ'].astype(str).str.replace('.0', '', regex=False).str.zfill(14).tolist()
        
        logger.info(f"Total de CNPJs carregados: {len(cnpjs)}")
        print(f"✅ {len(cnpjs)} CNPJs carregados")
        print(f"⏱️  Tempo estimado: ~{len(cnpjs) * settings.DELAY_ENTRE_REQUISICOES / 60:.1f} minutos\n")
        
        # 2. Executar ETL completo
        etl_service = FIDCETLService()
        logger.info("Iniciando pipeline ETL...")
        
        df_resultado = etl_service.process_and_validate(cnpjs, show_progress=True)
        
        # 3. Executar QA e gerar relatórios
        qa_service = QAService()
        logger.info("Executando pipeline de QA...")
        
        results = qa_service.full_qa_pipeline(df_resultado, str(settings.OUTPUTS_DIR))
        
        # 4. Resumo final
        print("\n" + "=" * 80)
        print("✅ ETL CONCLUÍDO COM SUCESSO!")
        print("=" * 80)
        print(f"\n📊 Estatísticas:")
        print(f"   Total processado: {len(df_resultado):,}")
        print(f"   Sucesso: {(df_resultado['STATUS'] == 'SUCESSO').sum():,}")
        print(f"   Erros: {(df_resultado['STATUS'] != 'SUCESSO').sum():,}")
        
        print(f"\n📁 Arquivos gerados em: {settings.OUTPUTS_DIR}")
        print(f"   ✅ cleaned_snapshot_YYYYMMDD_HHMMSS.csv - Dados completos validados (datado)")
        print(f"   ⚠️  qa_issues.csv - Registros com issues de qualidade")
        print(f"   📝 etl_fidc.log - Log detalhado da execução")
        
        logger.info("Pipeline concluído com sucesso!")
        return 0
        
    except Exception as e:
        logger.exception("Erro fatal durante execução do ETL")
        print(f"\n❌ ERRO FATAL: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

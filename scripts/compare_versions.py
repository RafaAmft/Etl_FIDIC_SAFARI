"""
Script para comparar duas versões de dados FIDC.

Uso:
    python compare_versions.py outputs/cleaned_snapshot_v1.csv outputs/cleaned_snapshot_v2.csv

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

from src.validators.diff_generator import DiffGenerator
from src.utils.logging_config import setup_logging
from src.config import settings


def main():
    """Compara duas versões de dados FIDC."""
    
    # Configurar logging
    setup_logging(log_level="INFO")
    logger = logging.getLogger(__name__)
    
    print("=" * 80)
    print("📊 Comparação de Versões - FIDC")
    print("=" * 80)
    print()
    
    # Verificar argumentos
    if len(sys.argv) < 3:
        print("❌ ERRO: Dois arquivos CSV são necessários")
        print("\nUso:")
        print("  python compare_versions.py <arquivo_v1.csv> <arquivo_v2.csv>")
        print("\nExemplo:")
        print("  python compare_versions.py outputs/snapshot1.csv outputs/snapshot2.csv")
        return 1
    
    file_v1 = Path(sys.argv[1])
    file_v2 = Path(sys.argv[2])
    
    # Verificar existência dos arquivos
    if not file_v1.exists():
        logger.error(f"Arquivo não encontrado: {file_v1}")
        print(f"❌ Arquivo não encontrado: {file_v1}")
        return 1
    
    if not file_v2.exists():
        logger.error(f"Arquivo não encontrado: {file_v2}")
        print(f"❌ Arquivo não encontrado: {file_v2}")
        return 1
    
    try:
        # Carregar DataFrames
        logger.info(f"Carregando {file_v1}")
        df_v1 = pd.read_csv(file_v1, sep=';', decimal=',')
        print(f"✅ Versão 1 carregada: {len(df_v1)} registros")
        
        logger.info(f"Carregando {file_v2}")
        df_v2 = pd.read_csv(file_v2, sep=';', decimal=',')
        print(f"✅ Versão 2 carregada: {len(df_v2)} registros")
        
        # Gerar relatório de diferenças
        print("\n🔍 Comparando versões...")
        diff_generator = DiffGenerator()
        
        output_file = settings.OUTPUTS_DIR / "diff_v1_v2.csv"
        df_diff = diff_generator.generate_diff_report(df_v1, df_v2, str(output_file))
        
        # Resumo
        print("\n" + "=" * 80)
        print("✅ COMPARAÇÃO CONCLUÍDA")
        print("=" * 80)
        
        if df_diff.empty:
            print("\n🎉 Nenhuma diferença encontrada!")
        else:
            print(f"\n⚠️  {len(df_diff)} diferenças encontradas")
            print(f"\n📁 Relatório salvo em: {output_file}")
            
            # Mostrar preview
            print("\n📋 Preview das diferenças:")
            print(df_diff.head(10).to_string(index=False))
        
        return 0
        
    except Exception as e:
        logger.exception("Erro durante comparação")
        print(f"\n❌ ERRO: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

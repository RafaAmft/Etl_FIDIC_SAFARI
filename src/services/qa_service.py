"""
Serviço de Quality Assurance para validações e relatórios.

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import pandas as pd
import logging
from pathlib import Path

from ..validators.data_validator import FIDCValidator
from ..validators.diff_generator import DiffGenerator
from ..loaders.file_exporter import FileExporter


logger = logging.getLogger(__name__)


class QAService:
    """Serviço de Quality Assurance e validações."""
    
    def __init__(self):
        """Inicializa o serviço com validador, diff generator e exporter."""
        self.validator = FIDCValidator()
        self.diff_generator = DiffGenerator()
        self.exporter = FileExporter()
    
    def run_all_validations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executa todas as validações de QA.
        
        Args:
            df: DataFrame com dados FIDC
            
        Returns:
            DataFrame com flags de validação adicionadas
        """
        logger.info("Executando validações de QA...")
        return self.validator.apply_all_validations(df)
    
    def generate_qa_report(
        self,
        df: pd.DataFrame,
        output_dir: str = "outputs"
    ) -> bool:
        """
        Gera relatório de issues de QA.
        
        Args:
            df: DataFrame validado
            output_dir: Diretório de saída
            
        Returns:
            True se sucesso
        """
        logger.info("Gerando relatório de QA...")
        return self.exporter.export_qa_issues(df, output_dir)
    
    def export_cleaned_snapshot(
        self,
        df: pd.DataFrame,
        output_dir: str = "outputs"
    ) -> bool:
        """
        Exporta snapshot limpo.
        
        Args:
            df: DataFrame validado
            output_dir: Diretório de saída
            
        Returns:
            True se sucesso
        """
        logger.info("Exportando snapshot limpo...")
        return self.exporter.export_cleaned_snapshot(df, output_dir)
    
    def compare_versions(
        self,
        df_v1: pd.DataFrame,
        df_v2: pd.DataFrame,
        output_dir: str = "outputs"
    ) -> pd.DataFrame:
        """
        Compara duas versões de dados e gera relatório de diferenças.
        
        Args:
            df_v1: DataFrame versão 1
            df_v2: DataFrame versão 2
            output_dir: Diretório de saída
            
        Returns:
            DataFrame com diferenças
        """
        logger.info("Comparando versões...")
        
        # Criar diretório se não existir
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Gerar relatório
        output_file = Path(output_dir) / "diff_v1_v2.csv"
        df_diff = self.diff_generator.generate_diff_report(
            df_v1,
            df_v2,
            str(output_file)
        )
        
        return df_diff
    
    def full_qa_pipeline(
        self,
        df: pd.DataFrame,
        output_dir: str = "outputs"
    ) -> dict:
        """
        Pipeline completo de QA: validações + relatórios.
        
        Args:
            df: DataFrame com dados FIDC
            output_dir: Diretório de saída
            
        Returns:
            Dicionário com resultados de cada etapa
        """
        results = {}
        
        # 1. Validações
        logger.info("Pipeline QA - Etapa 1/3: Validações")
        df_validated = self.run_all_validations(df)
        results['df_validated'] = df_validated
        
        # 2. Relatório de issues
        logger.info("Pipeline QA - Etapa 2/3: Relatório de issues")
        results['qa_report'] = self.generate_qa_report(df_validated, output_dir)
        
        # 3. Snapshot limpo
        logger.info("Pipeline QA - Etapa 3/3: Snapshot limpo")
        results['snapshot'] = self.export_cleaned_snapshot(df_validated, output_dir)
        
        logger.info("Pipeline QA concluído!")
        
        return results

"""
Exportador de arquivos CSV e Excel.

Adaptado para exportar 90+ campos com formatação compatível (Excel/PowerBI).

Autor: Rafael Augusto
Data: Janeiro 2026
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from ..config import settings


logger = logging.getLogger(__name__)


class FileExporter:
    """Exporta dados FIDC para CSV e Excel com formatação correta."""
    
    def __init__(self):
        """Inicializa o exportador."""
        # Forçar padrão "Golden Source" do notebook
        self.encoding = 'utf-8-sig' # BOM para Excel abrir direito
        self.sep = ';'
        self.decimal = ','
        self.float_format = '%.4f' # 4 casas decimais
    
    def export_to_csv(
        self,
        df: pd.DataFrame,
        output_path: str,
        index: bool = False
    ) -> bool:
        """
        Exporta DataFrame para CSV com formatação brasileira (PT-BR).
        """
        try:
            df.to_csv(
                output_path,
                index=index,
                sep=self.sep,
                decimal=self.decimal,
                float_format=self.float_format,
                encoding=self.encoding
            )
            logger.info(f"CSV exportado com sucesso: {output_path}")
            logger.info(f"  Linhas: {len(df):,} | Colunas: {len(df.columns)}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao exportar CSV {output_path}: {e}")
            return False
    
    def export_qa_issues(
        self,
        df: pd.DataFrame,
        output_dir: str = "outputs"
    ) -> bool:
        """Exporta relatório de issues de QA."""
        try:
            # Lista de flags de issues conhecidas (Atualizado)
            issue_cols = [
                'ATIVO_ZERO_FLAG',
                'CARTEIRA_BRUTA_ZERO_COM_INAD_FLAG',
                'SEM_POSICAO_FLAG'
            ]
            
            # Verificar se colunas existem (o df pode ter colunas variáveis agora)
            cols_present = [c for c in issue_cols if c in df.columns]
            
            if not cols_present:
                return True
                
            has_issues = df[cols_present].any(axis=1)
            df_issues = df[has_issues].copy()
            
            if df_issues.empty:
                logger.warning("Nenhum issue encontrado. Relatório QA ignorado.")
                return True
            
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            output_path = Path(output_dir) / "qa_issues.csv"
            
            return self.export_to_csv(df_issues, str(output_path))
            
        except Exception as e:
            logger.error(f"Erro ao exportar relatório QA: {e}")
            return False
    
    def export_cleaned_snapshot(
        self,
        df: pd.DataFrame,
        output_dir: str = "outputs"
    ) -> bool:
        """
        Exporta snapshot limpo completo (cleaned_snapshot_YYYYMMDD_HHMMSS.csv).
        """
        try:
            # Criar diretório se não existir
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Gerar nome com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"cleaned_snapshot_{timestamp}.csv"
            
            # Exportar
            output_path = Path(output_dir) / filename
            success = self.export_to_csv(df, str(output_path))
            
            if success:
                logger.info(f"Snapshot limpo exportado: {filename} ({len(df)} registros)")
                
                # Também salvar uma cópia "latest" para facilidade
                latest_path = Path(output_dir) / "cleaned_snapshot_latest.csv"
                self.export_to_csv(df, str(latest_path))
            
            return success
            
        except Exception as e:
            logger.error(f"Erro ao exportar snapshot limpo: {e}")
            return False

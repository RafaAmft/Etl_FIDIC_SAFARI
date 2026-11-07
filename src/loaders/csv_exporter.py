"""
Exportador de dados para CSV.
"""
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
from src.core.config import paths_config

logger = logging.getLogger(__name__)


class CSVExporter:
    """Exportador de dados para CSV"""
    
    def __init__(self, output_dir: Path = None):
        """
        Args:
            output_dir: Diretório de saída (se None, usa configuração padrão)
        """
        self.output_dir = output_dir or paths_config.get_output_subdir()
    
    def _prepare_dataframe(
        self,
        dados: List[Dict[str, Any]],
        filter_success: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Converte lista de dicts em DataFrame com validação.
        
        Args:
            dados: Lista de dicionários
            filter_success: Se True, filtra apenas STATUS == 'SUCESSO'
            
        Returns:
            DataFrame validado ou None
        """
        if not dados:
            return None
        
        df = pd.DataFrame(dados)
        
        # Filtrar registros válidos (com CNPJ_FUNDO)
        if 'CNPJ_FUNDO' in df.columns:
            df = df[
                df['CNPJ_FUNDO'].notna() & 
                ~df['CNPJ_FUNDO'].astype(str).isin(['', '0', '0.0'])
            ]
        
        if filter_success and 'STATUS' in df.columns:
            df = df[df['STATUS'] == 'SUCESSO']
        
        if df.empty:
            return None
        
        return df
    
    def _dedup_and_export(
        self,
        df: pd.DataFrame,
        filepath: Path,
        dedup_cols: List[str] = None,
        sort_cols: List[str] = None,
        sort_ascending: bool = False
    ) -> Path:
        """
        Remove duplicatas e exporta para CSV.
        
        Args:
            df: DataFrame a exportar
            filepath: Caminho de saída
            dedup_cols: Colunas para dedup (opcional)
            sort_cols: Colunas para ordenação (opcional)
            sort_ascending: Ordem de sort
            
        Returns:
            Path do arquivo salvo
        """
        total_antes = len(df)
        
        if dedup_cols:
            existing_cols = [c for c in dedup_cols if c in df.columns]
            if existing_cols:
                df = df.drop_duplicates(subset=existing_cols, keep='first')
                removidas = total_antes - len(df)
                if removidas > 0:
                    logger.info(f"   Duplicatas removidas: {removidas}")
        
        if sort_cols:
            existing_sort = [c for c in sort_cols if c in df.columns]
            if existing_sort:
                df = df.sort_values(existing_sort, ascending=sort_ascending)
        
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        return filepath
    
    def export_monitor_completo(
        self,
        dados: List[Dict[str, Any]],
        timestamp: str = None
    ) -> Optional[Path]:
        """
        Exporta dados completos para CSV (sem duplicidade).
        
        Args:
            dados: Lista de dicionários com dados dos fundos
            timestamp: Timestamp customizado
            
        Returns:
            Path: Caminho do arquivo salvo
        """
        df = self._prepare_dataframe(dados, filter_success=True)
        if df is None:
            logger.warning("Nenhum dado válido para exportar (monitor completo)")
            return None
        
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        filename = f"fidc_monitor_completo_{timestamp}.csv"
        filepath = self.output_dir / filename
        
        self._dedup_and_export(
            df, filepath,
            dedup_cols=['CNPJ_FUNDO', 'DATA_COMPETENCIA', 'CLASSE_SELECIONADA'],
            sort_cols=['CNPJ_FUNDO', 'DATA_COMPETENCIA']
        )
        
        logger.info(f"✅ Monitor completo salvo: {filepath} ({len(df)} linhas)")
        return filepath
    
    def export_safari_oportunidades(
        self,
        dados: List[Dict[str, Any]],
        score_min: float = 30.0,
        timestamp: str = None
    ) -> Optional[Path]:
        """
        Exporta apenas oportunidades do Safari (score >= threshold).
        """
        df = self._prepare_dataframe(dados, filter_success=True)
        if df is None:
            logger.warning("Nenhum dado para exportar (safari)")
            return None
        
        # Garantir tipos numéricos
        for col in ['SCORE_SAFARI', 'NPL_RATIO', 'ATIVO_TOTAL']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Filtrar oportunidades
        if 'SCORE_SAFARI' in df.columns:
            df = df[df['SCORE_SAFARI'] >= score_min]
        
        if df.empty:
            logger.warning(f"Nenhuma oportunidade com score >= {score_min}")
            return None
        
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        filename = f"fidc_safari_oportunidades_{timestamp}.csv"
        filepath = self.output_dir / filename
        
        self._dedup_and_export(
            df, filepath,
            sort_cols=['SCORE_SAFARI'],
            sort_ascending=False
        )
        
        logger.info(
            f"✅ Safari oportunidades salvo: {filepath} ({len(df)} linhas)"
        )
        return filepath
    
    def export_distressed_npl(
        self,
        dados: List[Dict[str, Any]],
        npl_min: float = 20.0,
        timestamp: str = None
    ) -> Optional[Path]:
        """
        Exporta fundos distressed com NPL alto.
        """
        df = self._prepare_dataframe(dados, filter_success=True)
        if df is None:
            logger.warning("Nenhum dado para exportar (distressed)")
            return None
        
        # Garantir tipo numérico
        if 'NPL_RATIO' in df.columns:
            df['NPL_RATIO'] = pd.to_numeric(df['NPL_RATIO'], errors='coerce').fillna(0)
            df = df[df['NPL_RATIO'] >= npl_min]
        
        if df.empty:
            logger.warning(f"Nenhum fundo distressed com NPL >= {npl_min}")
            return None
        
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        filename = f"fidc_distressed_npl_gt_{int(npl_min)}_{timestamp}.csv"
        filepath = self.output_dir / filename
        
        self._dedup_and_export(
            df, filepath,
            sort_cols=['NPL_RATIO'],
            sort_ascending=False
        )
        
        logger.info(
            f"✅ Distressed NPL salvo: {filepath} ({len(df)} linhas)"
        )
        return filepath

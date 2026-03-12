"""
Configurações centralizadas do projeto FIDC ETL.
"""
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class APIConfig:
    """Configurações da API B3"""
    url_busca: str = "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
    url_download: str = "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"
    timeout_busca: int = 10
    timeout_download: int = 20
    max_retries: int = 3
    delay_entre_requisicoes: float = 2.0
    headers: Dict[str, str] = field(default_factory=lambda: {
        "User-Agent": "Mozilla/5.0 (ETL FIDC Monitor v8)"
    })


@dataclass
class ETLConfig:
    """Configurações do ETL"""
    max_workers: int = 5
    rolling_window_months: int = 60
    cache_enabled: bool = True
    cache_version: str = "v8.0"  # v8: JSON cache, SHA256 keys, novo pipeline
    
    # Filtros FIDC Safari
    npl_min: float = 15.0
    npl_max: float = 150.0
    ativo_min: float = 1_000_000
    score_min: float = 30.0


@dataclass
class PathsConfig:
    """Configurações de caminhos"""
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    
    def __post_init__(self):
        self.data_dir = self.project_root / "data"
        self.input_dir = self.data_dir / "input"
        self.output_dir = self.data_dir / "output"
        self.cache_dir = self.data_dir / "cache"
        self.logs_dir = self.project_root / "logs"
        
        # Fallback: também aceitar pasta RAW/ para compatibilidade com V6
        self.raw_dir = self.project_root / "RAW"
        
        # Criar diretórios se não existem
        for path in [self.input_dir, self.output_dir, self.cache_dir, self.logs_dir]:
            path.mkdir(parents=True, exist_ok=True)
    
    def get_input_file(self, filename: str = "lista_cnpjs_fidc.csv") -> Path:
        """
        Localiza arquivo de entrada, verificando data/input/ e RAW/ (fallback).
        
        Returns:
            Path do arquivo encontrado
            
        Raises:
            FileNotFoundError se não encontrado em nenhum local
        """
        primary = self.input_dir / filename
        fallback = self.raw_dir / filename
        
        if primary.exists():
            return primary
        elif fallback.exists():
            return fallback
        else:
            raise FileNotFoundError(
                f"Arquivo '{filename}' não encontrado em:\n"
                f"  - {primary}\n"
                f"  - {fallback}\n"
                f"Coloque o arquivo em uma dessas pastas."
            )
    
    def get_output_subdir(self, date_str: str = None) -> Path:
        """Retorna subdiretório de output para uma data específica"""
        if date_str is None:
            from datetime import datetime
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        output_subdir = self.output_dir / date_str
        output_subdir.mkdir(parents=True, exist_ok=True)
        return output_subdir


# Instâncias globais (singleton)
api_config = APIConfig()
etl_config = ETLConfig()
paths_config = PathsConfig()

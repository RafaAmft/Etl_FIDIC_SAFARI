"""
Configurações centralizadas do projeto FIDC ETL.

Carrega variáveis do arquivo .env via pydantic-settings.
Todas as configurações podem ser sobrescritas via variáveis de ambiente.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configurações do projeto carregadas do .env."""

    # --- API B3 ---
    b3_url_busca: str = (
        "https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados"
    )
    b3_url_download: str = (
        "https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento"
    )
    b3_timeout_busca: int = 10
    b3_timeout_download: int = 20
    b3_max_retries: int = 3
    b3_delay_requisicoes: float = 2.0
    b3_user_agent: str = "Mozilla/5.0 (ETL FIDC Monitor v8)"

    # --- ETL Pipeline ---
    etl_max_workers: int = 5
    etl_rolling_window_months: int = 60
    etl_cache_enabled: bool = True
    etl_cache_version: str = "v8.0"

    # --- Filtros de negócio ---
    etl_npl_min: float = 15.0
    etl_npl_max: float = 150.0
    etl_ativo_min: float = 1_000_000.0
    etl_score_min: float = 30.0

    # --- Logging ---
    log_level: str = "INFO"

    # --- Caminhos (não configuráveis via .env — derivados do projeto) ---
    project_root: Path = Path(__file__).parent.parent

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"log_level deve ser um de {allowed}, recebido: {v!r}")
        return upper

    @field_validator("etl_max_workers")
    @classmethod
    def validate_max_workers(cls, v: int) -> int:
        if v < 1 or v > 50:
            raise ValueError(f"etl_max_workers deve estar entre 1 e 50, recebido: {v}")
        return v

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def input_dir(self) -> Path:
        return self.data_dir / "input"

    @property
    def output_dir(self) -> Path:
        return self.data_dir / "output"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def logs_dir(self) -> Path:
        return self.project_root / "logs"

    @property
    def raw_dir(self) -> Path:
        """Compatibilidade com versões anteriores (pasta RAW/)."""
        return self.project_root / "RAW"

    @property
    def b3_headers(self) -> dict[str, str]:
        return {"User-Agent": self.b3_user_agent}

    def ensure_dirs(self) -> None:
        """Cria diretórios necessários se não existirem."""
        for path in [self.input_dir, self.output_dir, self.cache_dir, self.logs_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def get_input_file(self, filename: str = "lista_cnpjs_fidc.csv") -> Path:
        """
        Localiza arquivo de entrada verificando input_dir e raw_dir (fallback).

        Raises:
            FileNotFoundError: Se o arquivo não for encontrado em nenhum local.
        """
        primary = self.input_dir / filename
        fallback = self.raw_dir / filename

        if primary.exists():
            return primary
        if fallback.exists():
            return fallback

        raise FileNotFoundError(
            f"Arquivo '{filename}' não encontrado em:\n"
            f"  - {primary}\n"
            f"  - {fallback}\n"
            f"Coloque o arquivo em uma dessas pastas."
        )

    def get_output_subdir(self, date_str: Optional[str] = None) -> Path:
        """Retorna subdiretório de output para uma data específica."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        output_subdir = self.output_dir / date_str
        output_subdir.mkdir(parents=True, exist_ok=True)
        return output_subdir

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


# Instância singleton — importar este objeto em vez da classe
settings = Settings()
